//! Value semantics: text and binary input/output, casts, operators and
//! scalar functions, following PostgreSQL 16.
//!
//! The planner resolves operators and functions once through
//! [`resolve_operator`] / [`resolve_function`]; the returned [`Func`] holds
//! a plain function pointer that the executor calls per row.

mod binary;
mod cast;
pub mod datetime;
mod f_array;
mod f_datetime;
mod f_json;
mod f_math;
mod f_string;
mod io;
mod numeric;
mod ops;
mod pattern;
mod registry;
mod to_char;

#[cfg(test)]
mod tests;

pub use binary::{decode_binary, encode_binary};
pub use cast::{can_cast, cast, common_type, CastContext};
pub use io::{format_value, parse_value};
pub use registry::{function_exists, resolve_function, resolve_operator, Func, FuncImpl, Volatility};
