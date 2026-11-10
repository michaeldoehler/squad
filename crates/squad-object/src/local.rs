use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{check_key, missing, Error, ObjectMeta, ObjectStore, Result};

const TMP_PREFIX: &str = ".tmp-";

/// [`ObjectStore`] in a directory of the local file system.
///
/// An object is a file at `dir/<key>`. A put writes a temporary file,
/// syncs it, renames it into place and syncs the directory, so an object
/// is either completely there or not at all, also after a crash.
#[derive(Debug)]
pub struct LocalStore {
    dir: PathBuf,
    counter: AtomicU64,
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

impl LocalStore {
    /// Opens the store in `dir`, creating the directory if needed.
    pub fn new(dir: impl Into<PathBuf>) -> Result<LocalStore> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        Ok(LocalStore { dir, counter: AtomicU64::new(0) })
    }

    fn path(&self, key: &str) -> Result<PathBuf> {
        check_key(key)?;
        if key.split('/').any(|s| s.starts_with(TMP_PREFIX)) {
            return Err(Error::Invalid(format!("object key {key:?} uses a reserved name")));
        }
        Ok(self.dir.join(key))
    }

    /// Writes `data` to a synced temporary file next to `path`.
    fn write_tmp(&self, path: &Path, data: &[u8]) -> Result<PathBuf> {
        let parent = path.parent().unwrap_or(&self.dir);
        fs::create_dir_all(parent)?;
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        let tmp = parent.join(format!("{TMP_PREFIX}{}-{n}", std::process::id()));
        let mut f = OpenOptions::new().write(true).create(true).truncate(true).open(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
        Ok(tmp)
    }

    fn walk(&self, dir: &Path, prefix: &str, after: &str, out: &mut Vec<ObjectMeta>) -> Result<()> {
        let entries = match fs::read_dir(dir) {
            Ok(e) => e,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with(TMP_PREFIX) {
                continue;
            }
            let path = entry.path();
            let rel = path.strip_prefix(&self.dir).unwrap_or(&path);
            let key = rel.components().map(|c| c.as_os_str().to_string_lossy()).collect::<Vec<_>>().join("/");
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                // Only descend where keys with the prefix can live.
                let as_dir = format!("{key}/");
                if as_dir.starts_with(prefix) || prefix.starts_with(&as_dir) {
                    self.walk(&path, prefix, after, out)?;
                }
            } else if key.starts_with(prefix) && key.as_str() > after {
                out.push(ObjectMeta { key, size: entry.metadata()?.len() });
            }
        }
        Ok(())
    }
}

impl ObjectStore for LocalStore {
    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        let path = self.path(key)?;
        let tmp = self.write_tmp(&path, data)?;
        if let Err(e) = fs::rename(&tmp, &path) {
            let _ = fs::remove_file(&tmp);
            return Err(e.into());
        }
        sync_dir(path.parent().unwrap_or(&self.dir))?;
        Ok(())
    }

    fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<bool> {
        let path = self.path(key)?;
        let tmp = self.write_tmp(&path, data)?;
        // A hard link fails if the target exists, which makes creation
        // atomic and exclusive.
        let linked = fs::hard_link(&tmp, &path);
        let _ = fs::remove_file(&tmp);
        match linked {
            Ok(()) => {
                sync_dir(path.parent().unwrap_or(&self.dir))?;
                Ok(true)
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let path = self.path(key)?;
        match fs::read(&path) {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            // A key that is a prefix of other keys is a directory here,
            // and not an object.
            Err(_) if path.is_dir() => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
        let path = self.path(key)?;
        if path.is_dir() {
            return Err(missing(key));
        }
        let mut f = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(missing(key)),
            Err(e) => return Err(e.into()),
        };
        let size = f.metadata()?.len();
        if offset.checked_add(len).is_none_or(|end| end > size) {
            return Err(Error::Invalid(format!(
                "range {offset}+{len} lies beyond the end of object {key:?} ({size} bytes)"
            )));
        }
        f.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; len as usize];
        f.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        match fs::metadata(self.path(key)?) {
            Ok(m) if m.is_file() => Ok(Some(ObjectMeta { key: key.to_string(), size: m.len() })),
            Ok(_) => Ok(None),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        let mut out = Vec::new();
        self.walk(&self.dir, prefix, "", &mut out)?;
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    fn list_after(&self, prefix: &str, after: &str) -> Result<Vec<ObjectMeta>> {
        // The directory is still read in full, but only new files are
        // looked at.
        let mut out = Vec::new();
        self.walk(&self.dir, prefix, after, &mut out)?;
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    fn delete(&self, key: &str) -> Result<()> {
        let path = self.path(key)?;
        match fs::remove_file(&path) {
            Ok(()) => {
                sync_dir(path.parent().unwrap_or(&self.dir))?;
                Ok(())
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}
