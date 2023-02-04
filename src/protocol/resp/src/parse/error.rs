use std::fmt;

use bstr::BStr;
use thiserror::Error;

#[derive(Error)]
pub enum ParseError<'a> {
    #[error("ran out of data unexpectedly")]
    Incomplete,
    #[error("expected {:?}, got {:?} instead", BStr::new(.expected), BStr::new(.found))]
    InvalidLiteral {
        expected: &'static [u8],
        found: &'a [u8],
    },
    #[error("expected a number, got {:?} instead", BStr::new(.0))]
    InvalidNumber(&'a [u8]),
    #[error("expected a non-negative number, got {:?} instead", BStr::new(.0))]
    UnexpectedNegativeNumber(&'a [u8]),

    #[error("expected an array element, but array was too short")]
    ExpectedArrayElement,
    #[error("expected array to be completely parsed, but there were more elements")]
    UnexpectedArrayElement,

    #[error("expected {expected}, got {:?}", BStr::new(&[*.found]))]
    UnexpectedCharacter { expected: &'static str, found: u8 },

    #[error("expected {expected}, got {found} instead")]
    Custom {
        expected: &'static str,
        found: &'static str,
    },
}

impl<'a> fmt::Debug for ParseError<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Incomplete => f.write_str("Incomplete"),
            Self::InvalidLiteral { expected, found } => f
                .debug_struct("InvalidLiteral")
                .field("expected", &BStr::new(expected))
                .field("found", &BStr::new(found))
                .finish(),
            Self::InvalidNumber(val) => f
                .debug_tuple("InvalidNumber")
                .field(&BStr::new(val))
                .finish(),
            Self::UnexpectedNegativeNumber(val) => f
                .debug_tuple("UnexpectedNegativeNumber")
                .field(&BStr::new(val))
                .finish(),
            Self::ExpectedArrayElement => f.write_str("ExpectedArrayElement"),
            Self::UnexpectedArrayElement => f.write_str("UnexpectedArrayElement"),
            Self::UnexpectedCharacter { expected, found } => f
                .debug_struct("UnexpectedCharacter")
                .field("expected", &expected)
                .field("found", &BStr::new(&[*found]))
                .finish(),
            Self::Custom { expected, found } => f
                .debug_struct("Custom")
                .field("expected", &BStr::new(expected))
                .field("found", &BStr::new(found))
                .finish(),
        }
    }
}

pub type ParseResult<'a, T> = Result<T, ParseError<'a>>;
