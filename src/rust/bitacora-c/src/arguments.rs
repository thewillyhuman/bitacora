//! Checking what C passes in and writing what C gets back.
//!
//! # Why this file exists
//!
//! Every entry point receives raw pointers: handles, text, byte ranges, places to write results. Each one
//! has to be checked before it is used, and each use is `unsafe`. If every entry point did that itself,
//! the crate would have a hundred `unsafe` blocks to review, each slightly different. This file holds the
//! few ways a pointer is ever turned into something safe, so that the rules are written once and an entry
//! point reads as a list of its arguments.
//!
//! # What it contains
//!
//! - `handle_reference`: a pointer to a handle becomes a reference.
//! - `text_argument`, `optional_text_argument`: a C string becomes a `&str`.
//! - `bytes_argument`: a pointer and a length become a `&[u8]`.
//! - `OutputSlot`, `output_slot`, `optional_output_slot`: a place the caller wants a result written to.
//! - `into_handle_pointer`, `free_handle`: giving a handle to C and taking it back.
//! - `SizedStructure`, `read_sized_structure`, `SizedOutputSlot`, `sized_output_slot`: structures that
//!   carry their own size, so that fields can be added in a later version without breaking programs built
//!   against this one.
//!
//! # How it works
//!
//! What can be checked is checked: a null pointer where one is required and text that is not UTF-8 are
//! reported as `BITACORA_STATUS_INVALID_ARGUMENT`, naming the argument, and nothing is dereferenced. What
//! cannot be checked is the caller's promise, stated in the `# Safety` section of each function here and
//! repeated on the entry points: that a pointer which is not null points to what the function says it
//! does, and stays valid for the call.
//!
//! An output is validated before the call does its work and written after. A call that appended a record
//! must not then find that it has nowhere to report the sequence number.
//!
//! A sized structure starts with a `struct_size` field that the caller sets to `sizeof` of the structure
//! as its compiler knows it. The library copies only that many bytes in or out, and takes its default for
//! every field beyond them. A program compiled against an older, shorter structure therefore keeps
//! working with a newer library, and the other way round.
//!
//! ```
//! use bitacora_c::{BitacoraStatus, bitacora_reader_open};
//!
//! // A null pointer is refused by name; nothing behind it is touched.
//! let mut reader = std::ptr::null_mut();
//! // SAFETY: null pointers are allowed, and the output points to a live pointer.
//! let status = unsafe { bitacora_reader_open(std::ptr::null(), c"orders".as_ptr(), &raw mut reader) };
//! assert_eq!(status, BitacoraStatus::InvalidArgument);
//! assert!(reader.is_null());
//! ```
//!
//! # Invariants
//!
//! - No pointer from C is dereferenced before it was checked for null.
//! - No reference made here outlives the call it was made in: entry points do not store them.
//! - A handle is a `Box` that was turned into a raw pointer by `into_handle_pointer`, and is turned back
//!   exactly once, by `free_handle`.
//!
//! # See also
//!
//! - `guard.rs`: the boundary that turns the failures returned here into status codes.

use std::ffi::{CStr, c_char};
use std::ptr::{self, NonNull};

use crate::status::Failure;

/// Borrows the handle a pointer from C names.
///
/// # Arguments
///
/// * `handle` - The pointer C passed.
/// * `argument_name` - The name of the argument, for the message if it is null.
///
/// # Returns
///
/// A reference to the handle, for the duration of the call.
///
/// # Errors
///
/// Returns an invalid-argument [`Failure`] if `handle` is null.
///
/// # Safety
///
/// `handle` must be null, or a pointer this library returned for a handle of this type that has not been
/// freed, and nobody may free it while the returned reference is in use.
pub(crate) unsafe fn handle_reference<'handle, Handle>(
    handle: *const Handle,
    argument_name: &str,
) -> Result<&'handle Handle, Failure> {
    // SAFETY: the caller guarantees that a pointer which is not null names a live handle of this type.
    match unsafe { handle.as_ref() } {
        Some(handle) => Ok(handle),
        None => Err(Failure::invalid_argument(format!("{argument_name} is null"))),
    }
}

/// Reads a required C string as text.
///
/// # Arguments
///
/// * `text` - The pointer C passed.
/// * `argument_name` - The name of the argument, for the message if it cannot be used.
///
/// # Returns
///
/// The text, borrowed from the caller's memory for the duration of the call.
///
/// # Errors
///
/// Returns an invalid-argument [`Failure`] if `text` is null or is not valid UTF-8.
///
/// # Safety
///
/// `text` must be null, or point to a NUL-terminated string that stays valid and unchanged while the
/// returned text is in use.
pub(crate) unsafe fn text_argument<'text>(text: *const c_char, argument_name: &str) -> Result<&'text str, Failure> {
    // SAFETY: the caller's guarantee is exactly the one `optional_text_argument` asks for.
    match unsafe { optional_text_argument(text, argument_name) }? {
        Some(text) => Ok(text),
        None => Err(Failure::invalid_argument(format!("{argument_name} is null"))),
    }
}

/// Reads a C string that may be left out as text.
///
/// # Arguments
///
/// * `text` - The pointer C passed. Null means the caller left the argument out.
/// * `argument_name` - The name of the argument, for the message if it cannot be used.
///
/// # Returns
///
/// `None` if `text` is null, otherwise the text.
///
/// # Errors
///
/// Returns an invalid-argument [`Failure`] if the text is not valid UTF-8.
///
/// # Safety
///
/// `text` must be null, or point to a NUL-terminated string that stays valid and unchanged while the
/// returned text is in use.
pub(crate) unsafe fn optional_text_argument<'text>(
    text: *const c_char,
    argument_name: &str,
) -> Result<Option<&'text str>, Failure> {
    if text.is_null() {
        return Ok(None);
    }
    // SAFETY: the pointer is not null, so the caller guarantees it names a NUL-terminated string.
    let c_text = unsafe { CStr::from_ptr(text) };
    match c_text.to_str() {
        Ok(text) => Ok(Some(text)),
        Err(utf8_error) => Err(Failure::invalid_argument(format!(
            "{argument_name} is not valid UTF-8: {utf8_error}"
        ))),
    }
}

/// Borrows a range of bytes that C describes by its first byte and its length.
///
/// # Arguments
///
/// * `bytes` - The pointer to the first byte. Ignored, and allowed to be null, when `length` is 0.
/// * `length` - How many bytes there are.
/// * `argument_name` - The name of the argument, for the message if it cannot be used.
///
/// # Returns
///
/// The bytes, borrowed from the caller's memory for the duration of the call. Empty when `length` is 0.
///
/// # Errors
///
/// Returns an invalid-argument [`Failure`] if `bytes` is null and `length` is not 0.
///
/// # Safety
///
/// If `length` is not 0, `bytes` must be null or point to `length` readable bytes that stay valid and
/// unchanged while the returned slice is in use.
pub(crate) unsafe fn bytes_argument<'bytes>(
    bytes: *const u8,
    length: usize,
    argument_name: &str,
) -> Result<&'bytes [u8], Failure> {
    if length == 0 {
        return Ok(&[]);
    }
    if bytes.is_null() {
        return Err(Failure::invalid_argument(format!(
            "{argument_name} is null but its length is {length}"
        )));
    }
    // SAFETY: the pointer is not null, so the caller guarantees `length` readable bytes behind it.
    Ok(unsafe { std::slice::from_raw_parts(bytes, length) })
}

/// A place in the caller's memory that the call writes one result to.
///
/// It is obtained before the call does its work and written after, possibly more than once. It never
/// reads what the caller left there.
pub(crate) struct OutputSlot<Value> {
    /// Where the value goes. Not null; valid for writes, by the promise made to [`output_slot`].
    destination: NonNull<Value>,
}

impl<Value> Clone for OutputSlot<Value> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Value> Copy for OutputSlot<Value> {}

impl<Value> OutputSlot<Value> {
    /// Writes `value` to the caller's memory, replacing whatever was there without looking at it.
    ///
    /// # Arguments
    ///
    /// * `value` - The result to hand to the caller.
    pub(crate) fn write(self, value: Value) {
        // SAFETY: whoever made this slot promised that the destination is valid for writes of `Value` for
        // the duration of the call, and a slot does not outlive the call it was made in.
        unsafe { self.destination.as_ptr().write(value) };
    }
}

/// Accepts a required place to write a result to.
///
/// # Arguments
///
/// * `destination` - The pointer C passed.
/// * `argument_name` - The name of the argument, for the message if it is null.
///
/// # Returns
///
/// The slot to write the result to once the call has done its work.
///
/// # Errors
///
/// Returns an invalid-argument [`Failure`] if `destination` is null.
///
/// # Safety
///
/// `destination` must be null, or point to memory that is valid for a write of `Value`, properly aligned,
/// and stays so until the call returns.
pub(crate) unsafe fn output_slot<Value>(
    destination: *mut Value,
    argument_name: &str,
) -> Result<OutputSlot<Value>, Failure> {
    // SAFETY: the caller's guarantee is exactly the one `optional_output_slot` asks for.
    match unsafe { optional_output_slot(destination) } {
        Some(slot) => Ok(slot),
        None => Err(Failure::invalid_argument(format!("{argument_name} is null"))),
    }
}

/// Accepts a place to write a result to that the caller may leave out.
///
/// # Arguments
///
/// * `destination` - The pointer C passed. Null means the caller does not want this result.
///
/// # Returns
///
/// `None` if `destination` is null, otherwise the slot to write the result to.
///
/// # Safety
///
/// `destination` must be null, or point to memory that is valid for a write of `Value`, properly aligned,
/// and stays so until the call returns.
pub(crate) unsafe fn optional_output_slot<Value>(destination: *mut Value) -> Option<OutputSlot<Value>> {
    NonNull::new(destination).map(|destination| OutputSlot { destination })
}

/// Gives a handle to C: moves it to the heap and returns the pointer C will hold.
///
/// # Arguments
///
/// * `handle` - The handle to hand out.
///
/// # Returns
///
/// A pointer that stays valid until it is passed to [`free_handle`], which must happen exactly once.
pub(crate) fn into_handle_pointer<Handle>(handle: Handle) -> *mut Handle {
    Box::into_raw(Box::new(handle))
}

/// Takes a handle back from C and drops it. A null pointer is ignored.
///
/// # Arguments
///
/// * `handle` - The pointer C passed to a `free` function.
///
/// # Safety
///
/// `handle` must be null, or a pointer returned by [`into_handle_pointer`] for a handle of this type that
/// has not been freed before. No other call may be using the handle, and it must not be used afterwards.
pub(crate) unsafe fn free_handle<Handle>(handle: *mut Handle) {
    if handle.is_null() {
        return;
    }
    // SAFETY: the pointer is not null, so the caller guarantees it came from `into_handle_pointer`, was
    // not freed before and is not in use: this is the one place the box is rebuilt.
    drop(unsafe { Box::from_raw(handle) });
}

/// A structure C passes by pointer that begins with its own size.
///
/// # Safety
///
/// An implementing type must be `#[repr(C)]`, must begin with a `u32` field that holds the size of the
/// structure in bytes, must have no padding, and must consist only of integers and raw pointers, so that
/// every pattern of bytes is a valid value of it.
pub(crate) unsafe trait SizedStructure: Copy {
    /// The structure with every field at the value that means "the default": zero, or null.
    fn with_defaults() -> Self;
}

/// The size of the `struct_size` field that every sized structure begins with.
const STRUCT_SIZE_FIELD_BYTES: usize = size_of::<u32>();

/// Reads the size a sized structure declares for itself.
///
/// # Errors
///
/// Returns an invalid-argument [`Failure`] if the declared size cannot even hold the size field: the
/// caller forgot to set it.
///
/// # Safety
///
/// `structure` must point to at least four readable bytes.
unsafe fn declared_size<Structure: SizedStructure>(
    structure: *const Structure,
    argument_name: &str,
) -> Result<usize, Failure> {
    // SAFETY: the caller guarantees four readable bytes, and an unaligned read asks for nothing more.
    let declared_size = unsafe { structure.cast::<u32>().read_unaligned() };
    let declared_size = usize::try_from(declared_size).unwrap_or(usize::MAX);
    if declared_size < STRUCT_SIZE_FIELD_BYTES {
        return Err(Failure::invalid_argument(format!(
            "{argument_name}.struct_size is {declared_size}: set it to the size of the structure"
        )));
    }
    Ok(declared_size)
}

/// Reads a sized structure that C passes in, taking defaults for what the caller's version does not have.
///
/// # Arguments
///
/// * `structure` - The pointer C passed. Null means "every default".
/// * `argument_name` - The name of the argument, for the message if it cannot be used.
///
/// # Returns
///
/// The structure. Fields beyond the size the caller declared hold their defaults; bytes the caller
/// declared beyond the size this library knows are ignored.
///
/// # Errors
///
/// Returns an invalid-argument [`Failure`] if the structure's `struct_size` was not set.
///
/// # Safety
///
/// `structure` must be null, or point to as many readable bytes as its first four bytes, read as a `u32`,
/// say, and at least to four.
pub(crate) unsafe fn read_sized_structure<Structure: SizedStructure>(
    structure: *const Structure,
    argument_name: &str,
) -> Result<Structure, Failure> {
    let mut known_structure = Structure::with_defaults();
    if structure.is_null() {
        return Ok(known_structure);
    }
    // SAFETY: the pointer is not null, so the caller guarantees at least four readable bytes.
    let declared_size = unsafe { declared_size(structure, argument_name) }?;
    let copied_bytes = declared_size.min(size_of::<Structure>());
    // SAFETY: the source has `declared_size` readable bytes by the caller's guarantee and the destination
    // is a local value of `size_of::<Structure>()` bytes; `copied_bytes` exceeds neither. They do not
    // overlap. Every byte pattern is a valid `Structure`, which `SizedStructure` promises.
    unsafe {
        ptr::copy_nonoverlapping(
            structure.cast::<u8>(),
            (&raw mut known_structure).cast::<u8>(),
            copied_bytes,
        );
    };
    Ok(known_structure)
}

/// A sized structure in the caller's memory that the call fills with a result.
///
/// Like an [`OutputSlot`], it is obtained before the call does its work and written after. It remembers
/// how much of the structure the caller's version has room for.
pub(crate) struct SizedOutputSlot<Structure> {
    /// Where the structure goes. Not null; valid for writes of `writable_bytes` bytes, by the promise made
    /// to [`sized_output_slot`].
    destination: NonNull<Structure>,
    /// How many bytes are written: the size the caller declared or the size this library knows, whichever
    /// is smaller. Never less than the size field itself.
    writable_bytes: usize,
}

impl<Structure: SizedStructure> SizedOutputSlot<Structure> {
    /// Writes as much of `structure` as the caller's version has room for, and sets the caller's
    /// `struct_size` to the number of bytes written.
    ///
    /// # Arguments
    ///
    /// * `structure` - The values to hand to the caller. Its own `struct_size` is ignored.
    ///
    /// # Panics
    ///
    /// Never in practice: it would take a structure of 4 GiB.
    pub(crate) fn write(self, structure: Structure) {
        let written_size = u32::try_from(self.writable_bytes).expect("a structure is far smaller than 4 GiB");
        let destination = self.destination.as_ptr();
        // SAFETY: whoever made this slot promised `writable_bytes` writable bytes at the destination for
        // the duration of the call. The source is a local value of `size_of::<Structure>()` bytes without
        // padding; `writable_bytes` exceeds neither, and is at least four. They do not overlap.
        unsafe {
            ptr::copy_nonoverlapping(
                (&raw const structure).cast::<u8>(),
                destination.cast::<u8>(),
                self.writable_bytes,
            );
            destination.cast::<u32>().write_unaligned(written_size);
        };
    }
}

/// Accepts a sized structure that the caller wants filled with a result.
///
/// # Arguments
///
/// * `destination` - The pointer C passed, with `struct_size` already set by the caller.
/// * `argument_name` - The name of the argument, for the message if it cannot be used.
///
/// # Returns
///
/// The slot to write the structure to once the call has done its work. Nothing is written before that.
///
/// # Errors
///
/// Returns an invalid-argument [`Failure`] if `destination` is null or its `struct_size` was not set.
///
/// # Safety
///
/// `destination` must be null, or point to as many writable bytes as its first four bytes, read as a
/// `u32`, say, and at least to four, and stay so until the call returns.
pub(crate) unsafe fn sized_output_slot<Structure: SizedStructure>(
    destination: *mut Structure,
    argument_name: &str,
) -> Result<SizedOutputSlot<Structure>, Failure> {
    let Some(destination) = NonNull::new(destination) else {
        return Err(Failure::invalid_argument(format!("{argument_name} is null")));
    };
    // SAFETY: the pointer is not null, so the caller guarantees at least four readable bytes.
    let declared_size = unsafe { declared_size(destination.as_ptr().cast_const(), argument_name) }?;
    Ok(SizedOutputSlot {
        destination,
        writable_bytes: declared_size.min(size_of::<Structure>()),
    })
}

#[cfg(test)]
#[path = "arguments_test.rs"]
mod tests;
