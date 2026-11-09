//! File-system abstraction.
//!
//! Every byte the disk engine reads or writes goes through a [`Vfs`]. The
//! production implementation is [`RealVfs`]; tests use
//! [`SimVfs`](crate::SimVfs), which simulates power loss, torn writes and
//! I/O errors deterministically.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::Arc;

/// A random-access file.
pub trait VfsFile: Send + Sync {
    /// Reads up to `buf.len()` bytes at `offset`. Returns the number of
    /// bytes read; fewer than requested only at end of file.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;
    /// Writes all of `buf` at `offset`, extending the file if needed.
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<()>;
    /// Forces file contents to stable storage.
    fn sync(&self) -> io::Result<()>;
    /// Truncates or extends the file.
    fn set_len(&self, len: u64) -> io::Result<()>;
    /// Current file size in bytes.
    fn size(&self) -> io::Result<u64>;
}

/// Held while a database directory is open; releases the lock when dropped.
pub trait VfsLock: Send + Sync {}

/// A minimal file system.
pub trait Vfs: Send + Sync + 'static {
    /// Opens an existing file for reading and writing.
    fn open(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>>;
    /// Creates a file, truncating it if it already exists.
    fn create(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>>;
    fn exists(&self, path: &Path) -> io::Result<bool>;
    fn remove(&self, path: &Path) -> io::Result<()>;
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    /// Names (not paths) of the entries in `dir`. A missing directory
    /// yields an empty list.
    fn list(&self, dir: &Path) -> io::Result<Vec<String>>;
    /// Makes creations, removals and renames inside `dir` durable.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
    /// Takes an exclusive advisory lock on `path`, creating the file if
    /// needed. Fails with [`io::ErrorKind::WouldBlock`] when already held,
    /// including by the same process.
    fn lock(&self, path: &Path) -> io::Result<Box<dyn VfsLock>>;
}

/// [`Vfs`] backed by the operating system.
#[derive(Debug, Default, Clone, Copy)]
pub struct RealVfs;

struct RealFile(File);

#[cfg(unix)]
fn pread(f: &File, buf: &mut [u8], off: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(f, buf, off)
}
#[cfg(unix)]
fn pwrite(f: &File, buf: &[u8], off: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::write_at(f, buf, off)
}
#[cfg(windows)]
fn pread(f: &File, buf: &mut [u8], off: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(f, buf, off)
}
#[cfg(windows)]
fn pwrite(f: &File, buf: &[u8], off: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_write(f, buf, off)
}

impl VfsFile for RealFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let mut done = 0;
        while done < buf.len() {
            match pread(&self.0, &mut buf[done..], offset + done as u64) {
                Ok(0) => break,
                Ok(n) => done += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(done)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        let mut done = 0;
        while done < buf.len() {
            match pwrite(&self.0, &buf[done..], offset + done as u64) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => done += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn sync(&self) -> io::Result<()> {
        self.0.sync_data()
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.0.set_len(len)
    }

    fn size(&self) -> io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }
}

struct RealLock(File);
impl VfsLock for RealLock {}
impl Drop for RealLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.0);
    }
}

impl Vfs for RealVfs {
    fn open(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>> {
        Ok(Arc::new(RealFile(OpenOptions::new().read(true).write(true).open(path)?)))
    }

    fn create(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>> {
        let f = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(path)?;
        Ok(Arc::new(RealFile(f)))
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        path.try_exists()
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
        let rd = match fs::read_dir(dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut out = Vec::new();
        for entry in rd {
            if let Ok(name) = entry?.file_name().into_string() {
                out.push(name);
            }
        }
        Ok(out)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
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

    fn lock(&self, path: &Path) -> io::Result<Box<dyn VfsLock>> {
        let f = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        match fs2::FileExt::try_lock_exclusive(&f) {
            Ok(()) => Ok(Box::new(RealLock(f))),
            Err(e) => Err(io::Error::new(io::ErrorKind::WouldBlock, e)),
        }
    }
}
