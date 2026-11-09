//! B+tree over the buffer pool.
//!
//! Readers call the free functions [`get`] and [`scan`] while holding the
//! engine's tree lock in shared mode; mutations go through [`Writer`] under
//! the exclusive lock. Leaves are doubly linked for range scans in both
//! directions. Deletes never merge underfull pages, but a page that becomes
//! empty is unlinked and returned to the free list.

use std::ops::Bound;

use crate::page::{self, PageBuf, ValRef, CAPACITY, INLINE_MAX, OVERFLOW_CAP, T_FREE, T_INTERNAL, T_LEAF, T_OVERFLOW};
use crate::pool::Pool;
use crate::{Result, StorageError};

const MAX_DEPTH: usize = 64;

/// Mutable tree bookkeeping, persisted in the header page at checkpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Meta {
    pub root: u32,
    pub free_head: u32,
    pub page_count: u32,
}

fn corrupt(msg: impl Into<String>) -> StorageError {
    StorageError::Corruption(msg.into())
}

/// Index of the child of internal page `p` that covers `key`, and its id.
#[inline]
fn child_for(p: &PageBuf, key: &[u8]) -> (usize, u32) {
    let idx = match page::search(p, key) {
        Ok(i) => i + 1,
        Err(i) => i,
    };
    (idx, child_at(p, idx))
}

#[inline]
fn child_at(p: &PageBuf, idx: usize) -> u32 {
    if idx < page::nkeys(p) {
        page::child(p, idx)
    } else {
        page::next(p)
    }
}

fn read_value(pool: &Pool, p: &PageBuf, i: usize) -> Result<Vec<u8>> {
    match page::value(p, i) {
        ValRef::Inline(v) => Ok(v.to_vec()),
        ValRef::Overflow { len, first } => {
            let mut out = Vec::with_capacity(len);
            let mut pid = first;
            while out.len() < len {
                if pid == 0 {
                    return Err(corrupt("overflow chain ends early"));
                }
                let op = pool.get(pid)?;
                if page::page_type(&op) != T_OVERFLOW {
                    return Err(corrupt(format!("page {pid} is not an overflow page")));
                }
                let data = page::overflow_data(&op);
                if data.is_empty() || out.len() + data.len() > len {
                    return Err(corrupt(format!("overflow page {pid} has an invalid length")));
                }
                out.extend_from_slice(data);
                pid = page::next(&op);
            }
            Ok(out)
        }
    }
}

pub fn get(pool: &Pool, root: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let mut pid = root;
    for _ in 0..MAX_DEPTH {
        let p = pool.get(pid)?;
        match page::page_type(&p) {
            T_INTERNAL => pid = child_for(&p, key).1,
            T_LEAF => {
                return match page::search(&p, key) {
                    Ok(i) => read_value(pool, &p, i).map(Some),
                    Err(_) => Ok(None),
                }
            }
            t => return Err(corrupt(format!("unexpected page type {t} at page {pid}"))),
        }
    }
    Err(corrupt("tree is deeper than the maximum depth"))
}

/// Which way to descend when no key guides the search.
enum Edge {
    Left,
    Right,
}

fn find_leaf(pool: &Pool, root: u32, key: Option<&[u8]>, edge: Edge) -> Result<std::sync::Arc<PageBuf>> {
    let mut pid = root;
    for _ in 0..MAX_DEPTH {
        let p = pool.get(pid)?;
        match page::page_type(&p) {
            T_INTERNAL => {
                pid = match (key, &edge) {
                    (Some(k), _) => child_for(&p, k).1,
                    (None, Edge::Left) => child_at(&p, 0),
                    (None, Edge::Right) => page::next(&p),
                }
            }
            T_LEAF => return Ok(p),
            t => return Err(corrupt(format!("unexpected page type {t} at page {pid}"))),
        }
    }
    Err(corrupt("tree is deeper than the maximum depth"))
}

fn sibling(pool: &Pool, pid: u32) -> Result<std::sync::Arc<PageBuf>> {
    let p = pool.get(pid)?;
    if page::page_type(&p) != T_LEAF {
        return Err(corrupt(format!("leaf chain points at non-leaf page {pid}")));
    }
    Ok(p)
}

pub fn scan(
    pool: &Pool,
    root: u32,
    lo: Bound<&[u8]>,
    hi: Bound<&[u8]>,
    reverse: bool,
    limit: usize,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut out = Vec::new();
    if limit == 0 {
        return Ok(out);
    }
    if !reverse {
        let start = match lo {
            Bound::Included(k) | Bound::Excluded(k) => Some(k),
            Bound::Unbounded => None,
        };
        let mut p = find_leaf(pool, root, start, Edge::Left)?;
        let mut i = match lo {
            Bound::Included(k) => page::search(&p, k).unwrap_or_else(|i| i),
            Bound::Excluded(k) => match page::search(&p, k) {
                Ok(i) => i + 1,
                Err(i) => i,
            },
            Bound::Unbounded => 0,
        };
        loop {
            let n = page::nkeys(&p);
            while i < n {
                let k = page::key(&p, i);
                let past = match hi {
                    Bound::Included(h) => k > h,
                    Bound::Excluded(h) => k >= h,
                    Bound::Unbounded => false,
                };
                if past {
                    return Ok(out);
                }
                out.push((k.to_vec(), read_value(pool, &p, i)?));
                if out.len() == limit {
                    return Ok(out);
                }
                i += 1;
            }
            match page::next(&p) {
                0 => return Ok(out),
                next => p = sibling(pool, next)?,
            }
            i = 0;
        }
    } else {
        let start = match hi {
            Bound::Included(k) | Bound::Excluded(k) => Some(k),
            Bound::Unbounded => None,
        };
        let mut p = find_leaf(pool, root, start, Edge::Right)?;
        // `e` is one past the next cell to visit.
        let mut e = match hi {
            Bound::Included(k) => match page::search(&p, k) {
                Ok(i) => i + 1,
                Err(i) => i,
            },
            Bound::Excluded(k) => page::search(&p, k).unwrap_or_else(|i| i),
            Bound::Unbounded => page::nkeys(&p),
        };
        loop {
            while e > 0 {
                e -= 1;
                let k = page::key(&p, e);
                let past = match lo {
                    Bound::Included(l) => k < l,
                    Bound::Excluded(l) => k <= l,
                    Bound::Unbounded => false,
                };
                if past {
                    return Ok(out);
                }
                out.push((k.to_vec(), read_value(pool, &p, e)?));
                if out.len() == limit {
                    return Ok(out);
                }
            }
            match page::prev(&p) {
                0 => return Ok(out),
                prev => p = sibling(pool, prev)?,
            }
            e = page::nkeys(&p);
        }
    }
}

/// Picks the split index `k` (cells `..k` stay left) so that both halves fit
/// in a page and are as balanced as possible. `sizes` include the slot.
/// `skip_mid` is set for internal pages, where cell `k` moves up instead of
/// going to the right half.
fn choose_split(sizes: &[usize], skip_mid: bool) -> usize {
    let total: usize = sizes.iter().sum();
    let mut best = None;
    let mut left = 0;
    for (k, &size) in sizes.iter().enumerate() {
        // A leaf split needs at least one cell on each side.
        if skip_mid || k > 0 {
            let right = total - left - if skip_mid { size } else { 0 };
            if left <= CAPACITY && right <= CAPACITY {
                let diff = left.abs_diff(right);
                if match best {
                    Some((d, _)) => diff < d,
                    None => true,
                } {
                    best = Some((diff, k));
                }
            }
        }
        left += size;
    }
    best.expect("a page overflow always has a valid split point").1
}

/// Path from the root to a leaf: `(internal page id, child index taken)`.
type Path = Vec<(u32, usize)>;

pub struct Writer<'a> {
    pub pool: &'a Pool,
    pub meta: &'a mut Meta,
}

impl Writer<'_> {
    fn descend(&self, key: &[u8]) -> Result<(u32, Path)> {
        let mut path = Path::new();
        let mut pid = self.meta.root;
        for _ in 0..MAX_DEPTH {
            let p = self.pool.get(pid)?;
            match page::page_type(&p) {
                T_INTERNAL => {
                    let (idx, child) = child_for(&p, key);
                    path.push((pid, idx));
                    pid = child;
                }
                T_LEAF => return Ok((pid, path)),
                t => return Err(corrupt(format!("unexpected page type {t} at page {pid}"))),
            }
        }
        Err(corrupt("tree is deeper than the maximum depth"))
    }

    fn alloc(&mut self) -> Result<u32> {
        if self.meta.free_head != 0 {
            let pid = self.meta.free_head;
            let p = self.pool.get(pid)?;
            if page::page_type(&p) != T_FREE {
                return Err(corrupt(format!("free list points at in-use page {pid}")));
            }
            self.meta.free_head = page::next(&p);
            Ok(pid)
        } else {
            let pid = self.meta.page_count;
            self.meta.page_count =
                pid.checked_add(1).ok_or_else(|| StorageError::Invalid("data file is full".into()))?;
            Ok(pid)
        }
    }

    fn free(&mut self, pid: u32) {
        let mut p = page::new_page(T_FREE);
        page::set_next(&mut p, self.meta.free_head);
        self.pool.put(pid, p);
        self.meta.free_head = pid;
    }

    fn free_chain(&mut self, first: u32) -> Result<()> {
        let mut pid = first;
        let mut guard = 0u64;
        while pid != 0 {
            let p = self.pool.get(pid)?;
            if page::page_type(&p) != T_OVERFLOW {
                return Err(corrupt(format!("page {pid} is not an overflow page")));
            }
            let next = page::next(&p);
            self.free(pid);
            pid = next;
            guard += 1;
            if guard > u32::MAX as u64 {
                return Err(corrupt("overflow chain contains a cycle"));
            }
        }
        Ok(())
    }

    fn write_overflow(&mut self, val: &[u8]) -> Result<u32> {
        // Written back to front so each page can point at its successor.
        let mut next = 0;
        let chunks: Vec<&[u8]> = val.chunks(OVERFLOW_CAP).collect();
        for chunk in chunks.into_iter().rev() {
            let pid = self.alloc()?;
            self.pool.put(pid, page::new_overflow(chunk, next));
            next = pid;
        }
        Ok(next)
    }

    pub fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        let (leaf, mut path) = self.descend(key)?;
        let (found, old_chain) = {
            let p = self.pool.get(leaf)?;
            let found = page::search(&p, key);
            let chain = match found.map(|i| page::value(&p, i)) {
                Ok(ValRef::Overflow { first, .. }) => Some(first),
                _ => None,
            };
            (found, chain)
        };
        if let Some(first) = old_chain {
            self.free_chain(first)?;
        }
        if let Ok(i) = found {
            self.pool.update(leaf, |pg| page::remove_cell(pg, i))?;
        }
        let cell = if val.len() > INLINE_MAX {
            let first = self.write_overflow(val)?;
            page::leaf_cell_overflow(key, val.len(), first)
        } else {
            page::leaf_cell_inline(key, val)
        };
        let idx = found.unwrap_or_else(|i| i);
        if self.pool.update(leaf, |pg| page::insert_cell(pg, idx, &cell))? {
            return Ok(());
        }
        self.split_leaf(leaf, &mut path, idx, cell)
    }

    fn split_leaf(&mut self, leaf: u32, path: &mut Path, idx: usize, cell: Vec<u8>) -> Result<()> {
        let p = self.pool.get(leaf)?;
        let n = page::nkeys(&p);
        let mut cells: Vec<Vec<u8>> = (0..n).map(|i| page::raw_cell(&p, i).to_vec()).collect();
        cells.insert(idx, cell);
        let old_next = page::next(&p);
        let k = if idx == n && old_next == 0 {
            // Appending at the right edge of the tree: keep the left page
            // full so sequential loads produce densely packed leaves.
            n
        } else {
            let sizes: Vec<usize> = cells.iter().map(|c| c.len() + 2).collect();
            choose_split(&sizes, false)
        };
        let right_pid = self.alloc()?;
        let mut left = page::new_page(T_LEAF);
        page::set_prev(&mut left, page::prev(&p));
        page::set_next(&mut left, right_pid);
        for (i, c) in cells[..k].iter().enumerate() {
            assert!(page::insert_cell(&mut left, i, c));
        }
        let mut right = page::new_page(T_LEAF);
        page::set_prev(&mut right, leaf);
        page::set_next(&mut right, old_next);
        for (i, c) in cells[k..].iter().enumerate() {
            assert!(page::insert_cell(&mut right, i, c));
        }
        drop(p);
        self.pool.put(leaf, left);
        self.pool.put(right_pid, right);
        if old_next != 0 {
            self.pool.update(old_next, |pg| page::set_prev(pg, right_pid))?;
        }
        let sep = page::cell_key(&cells[k]).to_vec();
        self.insert_into_parent(path, leaf, sep, right_pid)
    }

    /// After `left` split off `right` at key `sep`, registers the new page
    /// in the parent, splitting upward as needed.
    fn insert_into_parent(&mut self, path: &mut Path, left: u32, sep: Vec<u8>, right: u32) -> Result<()> {
        let Some((ppid, idx)) = path.pop() else {
            let new_root = self.alloc()?;
            let mut p = page::new_page(T_INTERNAL);
            assert!(page::insert_cell(&mut p, 0, &page::internal_cell(&sep, left)));
            page::set_next(&mut p, right);
            self.pool.put(new_root, p);
            self.meta.root = new_root;
            return Ok(());
        };
        // The pointer at `idx` referred to `left`. It now refers to `right`,
        // and a new cell (sep -> left) goes in front of it.
        let cell = page::internal_cell(&sep, left);
        let fits = self.pool.update(ppid, |pg| {
            if page::free_total(pg) < cell.len() + 2 {
                return false;
            }
            if idx < page::nkeys(pg) {
                page::set_child(pg, idx, right);
            } else {
                page::set_next(pg, right);
            }
            assert!(page::insert_cell(pg, idx, &cell));
            true
        })?;
        if fits {
            return Ok(());
        }
        let p = self.pool.get(ppid)?;
        let n = page::nkeys(&p);
        let mut cells: Vec<(Vec<u8>, u32)> = (0..n).map(|i| (page::key(&p, i).to_vec(), page::child(&p, i))).collect();
        let mut rightmost = page::next(&p);
        drop(p);
        if idx < n {
            cells[idx].1 = right;
        } else {
            rightmost = right;
        }
        cells.insert(idx, (sep, left));
        let sizes: Vec<usize> = cells.iter().map(|(k, _)| 6 + k.len() + 2).collect();
        let m = choose_split(&sizes, true);
        let build = |cells: &[(Vec<u8>, u32)], rightmost: u32| {
            let mut pg = page::new_page(T_INTERNAL);
            for (i, (k, c)) in cells.iter().enumerate() {
                assert!(page::insert_cell(&mut pg, i, &page::internal_cell(k, *c)));
            }
            page::set_next(&mut pg, rightmost);
            pg
        };
        let new_right = self.alloc()?;
        self.pool.put(ppid, build(&cells[..m], cells[m].1));
        self.pool.put(new_right, build(&cells[m + 1..], rightmost));
        let promoted = std::mem::take(&mut cells[m].0);
        self.insert_into_parent(path, ppid, promoted, new_right)
    }

    /// Removes `key`. Returns whether it existed.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        let (leaf, mut path) = self.descend(key)?;
        let (i, old_chain, prev, next) = {
            let p = self.pool.get(leaf)?;
            let Ok(i) = page::search(&p, key) else {
                return Ok(false);
            };
            let chain = match page::value(&p, i) {
                ValRef::Overflow { first, .. } => Some(first),
                ValRef::Inline(_) => None,
            };
            (i, chain, page::prev(&p), page::next(&p))
        };
        if let Some(first) = old_chain {
            self.free_chain(first)?;
        }
        let empty = self.pool.update(leaf, |pg| {
            page::remove_cell(pg, i);
            page::nkeys(pg) == 0
        })?;
        if empty && !path.is_empty() {
            if prev != 0 {
                self.pool.update(prev, |pg| page::set_next(pg, next))?;
            }
            if next != 0 {
                self.pool.update(next, |pg| page::set_prev(pg, prev))?;
            }
            self.free(leaf);
            self.remove_child(&mut path)?;
        }
        Ok(true)
    }

    /// Drops the child pointer recorded at the end of `path` from its
    /// parent, cascading when internal pages run empty.
    fn remove_child(&mut self, path: &mut Path) -> Result<()> {
        let (ppid, idx) = path.pop().expect("remove_child needs a parent");
        let n = page::nkeys(&*self.pool.get(ppid)?);
        if n == 0 {
            // The parent's only child is gone.
            if path.is_empty() {
                self.pool.put(ppid, page::new_page(T_LEAF));
                return Ok(());
            }
            self.free(ppid);
            return self.remove_child(path);
        }
        self.pool.update(ppid, |pg| {
            if idx < n {
                page::remove_cell(pg, idx);
            } else {
                let c = page::child(pg, n - 1);
                page::set_next(pg, c);
                page::remove_cell(pg, n - 1);
            }
        })?;
        if path.is_empty() {
            // Collapse a root that is left with a single child.
            loop {
                let root = self.meta.root;
                let p = self.pool.get(root)?;
                if page::page_type(&p) != T_INTERNAL || page::nkeys(&p) != 0 {
                    break;
                }
                self.meta.root = page::next(&p);
                drop(p);
                self.free(root);
            }
        }
        Ok(())
    }
}
