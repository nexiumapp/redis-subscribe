use nom::{
    branch::alt,
    bytes::streaming::tag_no_case,
    character::streaming::{char, crlf, i64, not_line_ending, u64},
    multi::count,
    sequence::{delimited, tuple},
    IResult,
};

#[derive(Debug, PartialEq)]
pub enum Response {
    Null,
    SimpleString(String),
    Error(String),
    Integer(i64),
    Bulk(String),
    Array(Vec<Response>),
}

type NomResult<'a, T> = IResult<&'a str, T>;

pub fn parse<'a>(input: &mut String) -> Vec<Response> {
    let mut result = Vec::new();

    loop {
        let (rem, res) = match parse_response(input.as_str()) {
            Ok(parsed) => parsed,
            Err(_) => return result,
        };

        result.push(res);
        *input = rem.to_string();
    }
}

fn parse_response(input: &str) -> NomResult<Response> {
    alt((
        parse_simple,
        parse_error,
        parse_integer,
        parse_bulk_string,
        parse_null,
        parse_array,
    ))(input)
}

fn parse_simple(input: &str) -> NomResult<Response> {
    let (rem, res) = delimited(char('+'), not_line_ending, crlf)(input)?;

    Ok((rem, Response::SimpleString(res.to_string())))
}

fn parse_error(input: &str) -> NomResult<Response> {
    let (rem, res) = delimited(char('-'), not_line_ending, crlf)(input)?;

    Ok((rem, Response::Error(res.to_string())))
}

fn parse_integer(input: &str) -> NomResult<Response> {
    let (rem, res) = delimited(char(':'), i64, crlf)(input)?;

    Ok((rem, Response::Integer(res)))
}

fn parse_bulk_string(input: &str) -> NomResult<Response> {
    let (rem, (_, _, _, data, _)) = tuple((char('$'), u64, crlf, not_line_ending, crlf))(input)?;

    Ok((rem, Response::Bulk(data.to_string())))
}

fn parse_null(input: &str) -> NomResult<Response> {
    let (rem, _) = tuple((tag_no_case("$-1"), crlf))(input)?;

    Ok((rem, Response::Null))
}

fn parse_array(input: &str) -> NomResult<Response> {
    let (rem, amount) = delimited(char('*'), u64, crlf)(input)?;
    let (rem, entries) = count(parse_response, amount as usize)(rem)?;

    Ok((rem, Response::Array(entries)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_string() {
        let (rem, res) = parse_response("+OK\r\n").unwrap();

        assert_eq!("", rem);
        assert_eq!(Response::SimpleString("OK".to_string()), res);
    }

    #[test]
    fn error() {
        let (rem, res) = parse_response("-Error message\r\n").unwrap();

        assert_eq!("", rem);
        assert_eq!(Response::Error("Error message".to_string()), res);
    }

    #[test]
    fn integer() {
        let (rem, res) = parse_response(":1000\r\n").unwrap();

        assert_eq!("", rem);
        assert_eq!(Response::Integer(1000), res);
    }

    #[test]
    fn bulk() {
        let (rem, res) = parse_response("$6\r\nfoobar\r\n").unwrap();

        assert_eq!("", rem);
        assert_eq!(Response::Bulk("foobar".to_string()), res);
    }

    #[test]
    fn null() {
        let (rem, res) = parse_response("$-1\r\n").unwrap();

        assert_eq!("", rem);
        assert_eq!(Response::Null, res);
    }

    #[test]
    fn array() {
        let (rem, res) = parse_response("*0\r\n").unwrap();

        assert_eq!("", rem);
        assert_eq!(Response::Array(vec![]), res);
    }

    #[test]
    fn array_filled() {
        let (rem, res) = parse_response("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n").unwrap();

        assert_eq!("", rem);
        assert_eq!(
            Response::Array(vec![
                Response::Bulk("foo".to_string()),
                Response::Bulk("bar".to_string())
            ]),
            res
        );
    }

    #[test]
    fn array_nested() {
        let (rem, res) =
            parse_response("*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n").unwrap();

        assert_eq!("", rem);
        assert_eq!(
            Response::Array(vec![
                Response::Array(vec![
                    Response::Integer(1),
                    Response::Integer(2),
                    Response::Integer(3),
                ]),
                Response::Array(vec![
                    Response::SimpleString("Foo".to_string()),
                    Response::Error("Bar".to_string())
                ])
            ]),
            res
        );
    }

    #[test]
    fn array_null() {
        let (rem, res) = parse_response("*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n").unwrap();

        assert_eq!("", rem);
        assert_eq!(
            Response::Array(vec![
                Response::Bulk("foo".to_string()),
                Response::Null,
                Response::Bulk("bar".to_string())
            ]),
            res
        );
    }
}
