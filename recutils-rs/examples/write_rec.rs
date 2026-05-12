//! Build a fresh `.rec` file from scratch and print it to stdout.
//!
//! Demonstrates [`Db::new`], [`OwnedRset`], [`OwnedRset::set_descriptor`],
//! and [`Db::to_rec_string`] — the write-side primitives that complement
//! [`Db::parse_str`].

use recutils_rs::{Db, OwnedRset, Record};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Db::new();

    let mut rset = OwnedRset::new();

    let mut descriptor = Record::new();
    descriptor.append_field("%rec", "Book")?;
    descriptor.append_field("%type", "Year int")?;
    descriptor.append_field("%mandatory", "Title")?;
    rset.set_descriptor(descriptor);

    for (title, author, year) in [
        ("Refactoring", "Martin Fowler", "1999"),
        ("Domain-Driven Design", "Eric Evans", "2003"),
        ("Test-Driven Development", "Kent Beck", "2002"),
    ] {
        let mut record = Record::new();
        record.append_field("Title", title)?;
        record.append_field("Author", author)?;
        record.append_field("Year", year)?;
        rset.append_record(record)?;
    }

    db.append_rset(rset)?;
    println!("{}", db.to_rec_string()?);
    Ok(())
}
