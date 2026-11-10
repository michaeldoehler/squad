//! Deterministic fault-injecting file system for simulation tests.
//!
//! [`SimVfs`] keeps every file in memory and tracks, per file, which bytes
//! have been made durable by `sync` and which writes are still pending.
//! Directory operations (create, remove, rename) are likewise pending until
//! `sync_dir`. A simulated power loss ([`SimVfs::crash`]) then decides, from
//! a seeded random generator, the fate of everything pending: dropped,
//! kept, kept up to a point, or torn at sector granularity.
//!
//! Faults are armed with [`SimVfs::crash_after`] (the process "dies" at the
//! n-th mutating operation; that operation is torn and every later call
//! fails) and [`SimVfs::set_error_rate`] (random I/O errors).

use std::collections::{BTreeMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::vfs::{Vfs, VfsFile, VfsLock};

const SECTOR: usize = 512;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        // splitmix64
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

enum Pending {
    Write { off: usize, data: Vec<u8> },
    SetLen(usize),
}

#[derive(Default)]
struct FileData {
    cur: Vec<u8>,
    durable: Vec<u8>,
    pending: Vec<Pending>,
}

fn apply_write(buf: &mut Vec<u8>, off: usize, data: &[u8]) {
    if buf.len() < off + data.len() {
        buf.resize(off + data.len(), 0);
    }
    buf[off..off + data.len()].copy_from_slice(data);
}

fn apply(buf: &mut Vec<u8>, p: &Pending) {
    match p {
        Pending::Write { off, data } => apply_write(buf, *off, data),
        Pending::SetLen(len) => buf.resize(*len, 0),
    }
}

/// Applies a random part of a write, sector by sector.
fn apply_torn(buf: &mut Vec<u8>, off: usize, data: &[u8], rng: &mut Rng) {
    let prefix_only = rng.below(2) == 0;
    let mut pos = 0;
    while pos < data.len() {
        let end = (((off + pos) / SECTOR + 1) * SECTOR - off).min(data.len());
        if rng.below(2) == 0 {
            apply_write(buf, off + pos, &data[pos..end]);
        } else if prefix_only {
            return;
        }
        pos = end;
    }
}

enum NsOp {
    Create(PathBuf, usize),
    Remove(PathBuf),
    Rename(PathBuf, PathBuf),
}

impl NsOp {
    fn dir(&self) -> Option<&Path> {
        match self {
            NsOp::Create(p, _) | NsOp::Remove(p) | NsOp::Rename(_, p) => p.parent(),
        }
    }
    fn apply(&self, names: &mut BTreeMap<PathBuf, usize>) {
        match self {
            NsOp::Create(p, id) => {
                names.insert(p.clone(), *id);
            }
            NsOp::Remove(p) => {
                names.remove(p);
            }
            NsOp::Rename(a, b) => {
                if let Some(id) = names.remove(a) {
                    names.insert(b.clone(), id);
                }
            }
        }
    }
}

struct State {
    rng: Rng,
    files: Vec<FileData>,
    names: BTreeMap<PathBuf, usize>,
    durable_names: BTreeMap<PathBuf, usize>,
    pending_ns: Vec<NsOp>,
    locks: HashSet<PathBuf>,
    lock_gen: u64,
    crashed: bool,
    ops: u64,
    crash_at: Option<u64>,
    /// Probability of an injected error per mutating operation, in parts
    /// per million.
    error_ppm: u64,
    injected_errors: u64,
}

enum Fate {
    Proceed,
    /// The process dies during this operation.
    Dying,
}

impl State {
    fn alive(&self) -> io::Result<()> {
        if self.crashed {
            return Err(io::Error::other("simulated crash: process is dead"));
        }
        Ok(())
    }

    fn tick(&mut self) -> io::Result<Fate> {
        self.alive()?;
        self.ops += 1;
        if self.crash_at == Some(self.ops) {
            self.crashed = true;
            return Ok(Fate::Dying);
        }
        if self.error_ppm > 0 && self.rng.below(1_000_000) < self.error_ppm {
            self.injected_errors += 1;
            return Err(io::Error::other("simulated I/O error"));
        }
        Ok(Fate::Proceed)
    }
}

fn dead() -> io::Error {
    io::Error::other("simulated crash: process died during this operation")
}

/// In-memory [`Vfs`] with deterministic fault injection. Cheap to clone.
#[derive(Clone)]
pub struct SimVfs {
    st: Arc<Mutex<State>>,
}

impl SimVfs {
    pub fn new(seed: u64) -> SimVfs {
        SimVfs {
            st: Arc::new(Mutex::new(State {
                rng: Rng(seed),
                files: Vec::new(),
                names: BTreeMap::new(),
                durable_names: BTreeMap::new(),
                pending_ns: Vec::new(),
                locks: HashSet::new(),
                lock_gen: 0,
                crashed: false,
                ops: 0,
                crash_at: None,
                error_ppm: 0,
                injected_errors: 0,
            })),
        }
    }

    /// Arms a crash at the `n`-th mutating operation from now (`n >= 1`).
    /// That operation is applied partially; all later operations fail until
    /// [`SimVfs::crash`] is called.
    pub fn crash_after(&self, n: u64) {
        let mut st = self.st.lock();
        st.crash_at = Some(st.ops + n.max(1));
    }

    /// Kills the simulated process right now: every later operation fails
    /// until [`SimVfs::crash`] is called.
    pub fn kill(&self) {
        self.st.lock().crashed = true;
    }

    /// Makes every mutating operation fail with the given probability
    /// (parts per million). A failed operation has no effect.
    pub fn set_error_rate(&self, ppm: u64) {
        self.st.lock().error_ppm = ppm;
    }

    /// Number of errors injected so far.
    pub fn injected_errors(&self) -> u64 {
        self.st.lock().injected_errors
    }

    /// Number of mutating operations performed so far.
    pub fn op_count(&self) -> u64 {
        self.st.lock().ops
    }

    /// Whether an armed crash has fired.
    pub fn is_crashed(&self) -> bool {
        self.st.lock().crashed
    }

    /// Simulates power loss and restart: resolves the fate of all unsynced
    /// writes and directory operations, releases all locks and disarms
    /// pending faults. File handles opened before the crash must not be
    /// used afterwards.
    pub fn crash(&self) {
        let mut guard = self.st.lock();
        let st = &mut *guard;
        for f in &mut st.files {
            let mut image = std::mem::take(&mut f.durable);
            let pending = std::mem::take(&mut f.pending);
            match st.rng.below(4) {
                0 => {}
                1 => pending.iter().for_each(|p| apply(&mut image, p)),
                2 => {
                    // A prefix survives intact; the next write may be torn.
                    let keep = st.rng.below(pending.len() as u64 + 1) as usize;
                    pending[..keep].iter().for_each(|p| apply(&mut image, p));
                    if let Some(Pending::Write { off, data }) = pending.get(keep) {
                        apply_torn(&mut image, *off, data, &mut st.rng);
                    }
                }
                _ => {
                    // Writes reach the disk in arbitrary subsets.
                    for p in &pending {
                        match (st.rng.below(3), p) {
                            (0, _) => {}
                            (1, Pending::Write { off, data }) => apply_torn(&mut image, *off, data, &mut st.rng),
                            _ => apply(&mut image, p),
                        }
                    }
                }
            }
            f.cur = image.clone();
            f.durable = image;
        }
        let pending_ns = std::mem::take(&mut st.pending_ns);
        let keep = match st.rng.below(3) {
            0 => 0,
            1 => pending_ns.len(),
            _ => st.rng.below(pending_ns.len() as u64 + 1) as usize,
        };
        for op in &pending_ns[..keep] {
            op.apply(&mut st.durable_names);
        }
        st.names = st.durable_names.clone();
        st.locks.clear();
        st.lock_gen += 1;
        st.crashed = false;
        st.crash_at = None;
        st.error_ppm = 0;
    }

    /// Flips one bit in the durable and current image of `path`, to test
    /// corruption detection. Returns false if the file is too short.
    pub fn corrupt_byte(&self, path: &Path, offset: u64) -> bool {
        let mut st = self.st.lock();
        let Some(&id) = st.names.get(path) else {
            return false;
        };
        let f = &mut st.files[id];
        let off = offset as usize;
        if off >= f.cur.len() || off >= f.durable.len() {
            return false;
        }
        f.cur[off] ^= 0x40;
        f.durable[off] ^= 0x40;
        true
    }
}

struct SimFile {
    st: Arc<Mutex<State>>,
    id: usize,
}

impl VfsFile for SimFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let st = self.st.lock();
        st.alive()?;
        let cur = &st.files[self.id].cur;
        let off = (offset as usize).min(cur.len());
        let n = buf.len().min(cur.len() - off);
        buf[..n].copy_from_slice(&cur[off..off + n]);
        Ok(n)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        let mut guard = self.st.lock();
        let st = &mut *guard;
        let off = offset as usize;
        match st.tick()? {
            Fate::Proceed => {
                let f = &mut st.files[self.id];
                apply_write(&mut f.cur, off, buf);
                f.pending.push(Pending::Write { off, data: buf.to_vec() });
                Ok(())
            }
            Fate::Dying => {
                // Part of the write may have left the process.
                let cut = st.rng.below(buf.len() as u64 + 1) as usize;
                let f = &mut st.files[self.id];
                apply_write(&mut f.cur, off, &buf[..cut]);
                f.pending.push(Pending::Write { off, data: buf[..cut].to_vec() });
                Err(dead())
            }
        }
    }

    fn sync(&self) -> io::Result<()> {
        let mut st = self.st.lock();
        match st.tick()? {
            Fate::Proceed => {
                let f = &mut st.files[self.id];
                let pending = std::mem::take(&mut f.pending);
                pending.iter().for_each(|p| apply(&mut f.durable, p));
                Ok(())
            }
            Fate::Dying => Err(dead()),
        }
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        let mut st = self.st.lock();
        match st.tick()? {
            Fate::Proceed => {
                let f = &mut st.files[self.id];
                f.cur.resize(len as usize, 0);
                f.pending.push(Pending::SetLen(len as usize));
                Ok(())
            }
            Fate::Dying => Err(dead()),
        }
    }

    fn size(&self) -> io::Result<u64> {
        let st = self.st.lock();
        st.alive()?;
        Ok(st.files[self.id].cur.len() as u64)
    }
}

struct SimLock {
    st: Arc<Mutex<State>>,
    path: PathBuf,
    gen: u64,
}
impl VfsLock for SimLock {}
impl Drop for SimLock {
    fn drop(&mut self) {
        let mut st = self.st.lock();
        if st.lock_gen == self.gen {
            st.locks.remove(&self.path);
        }
    }
}

impl Vfs for SimVfs {
    fn open(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>> {
        let st = self.st.lock();
        st.alive()?;
        match st.names.get(path) {
            Some(&id) => Ok(Arc::new(SimFile { st: self.st.clone(), id })),
            None => Err(io::ErrorKind::NotFound.into()),
        }
    }

    fn create(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>> {
        let mut st = self.st.lock();
        match st.tick()? {
            Fate::Proceed => {
                // Truncating an existing file is modelled as replacing it.
                st.files.push(FileData::default());
                let id = st.files.len() - 1;
                st.names.insert(path.to_path_buf(), id);
                st.pending_ns.push(NsOp::Create(path.to_path_buf(), id));
                Ok(Arc::new(SimFile { st: self.st.clone(), id }))
            }
            Fate::Dying => Err(dead()),
        }
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        let st = self.st.lock();
        st.alive()?;
        Ok(st.names.contains_key(path))
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        let mut st = self.st.lock();
        match st.tick()? {
            Fate::Proceed => {
                if st.names.remove(path).is_none() {
                    return Err(io::ErrorKind::NotFound.into());
                }
                st.pending_ns.push(NsOp::Remove(path.to_path_buf()));
                Ok(())
            }
            Fate::Dying => Err(dead()),
        }
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut st = self.st.lock();
        match st.tick()? {
            Fate::Proceed => {
                let Some(id) = st.names.remove(from) else {
                    return Err(io::ErrorKind::NotFound.into());
                };
                st.names.insert(to.to_path_buf(), id);
                st.pending_ns.push(NsOp::Rename(from.to_path_buf(), to.to_path_buf()));
                Ok(())
            }
            Fate::Dying => Err(dead()),
        }
    }

    fn create_dir_all(&self, _path: &Path) -> io::Result<()> {
        // Directories are implicit and always durable in the simulation.
        self.st.lock().alive()
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
        let st = self.st.lock();
        st.alive()?;
        Ok(st
            .names
            .keys()
            .filter(|p| p.parent() == Some(dir))
            .filter_map(|p| p.file_name()?.to_str().map(str::to_owned))
            .collect())
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let mut guard = self.st.lock();
        let st = &mut *guard;
        match st.tick()? {
            Fate::Proceed => {
                let pending = std::mem::take(&mut st.pending_ns);
                for op in pending {
                    if op.dir() == Some(dir) {
                        op.apply(&mut st.durable_names);
                    } else {
                        st.pending_ns.push(op);
                    }
                }
                Ok(())
            }
            Fate::Dying => Err(dead()),
        }
    }

    fn lock(&self, path: &Path) -> io::Result<Box<dyn VfsLock>> {
        let mut st = self.st.lock();
        st.alive()?;
        if !st.locks.insert(path.to_path_buf()) {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        Ok(Box::new(SimLock { st: self.st.clone(), path: path.to_path_buf(), gen: st.lock_gen }))
    }
}
