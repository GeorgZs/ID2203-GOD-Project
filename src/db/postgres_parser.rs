use crate::db::query_parser::Parse;
use crate::db::query_parser::DataSourceObject;
enum PG_Qtype {
    INSERT,
    UPDATE,
    READ
}

pub struct PGParser {
    db_type: PG_Qtype // TODO: how to use this?
}

impl Parse for PGParser {
    fn parse_insert(&self, object: DataSourceObject) -> String {
        let mut query_string = String::from("INSERT INTO ");
        query_string.push_str(&object.table_name);
        query_string.push_str(" (");

        //clone the data to avoid borrowing issues
        if(object.row_data.len() > 1) {
            let copy_data = object.row_data.iter().clone();

            for row in copy_data {
                query_string.push_str(&row.row_name);
                query_string.push_str(", ");
            }
            query_string.pop();
            query_string.pop();
            query_string.push_str(") VALUES (");
            for row in object.row_data {
                query_string.push_str(&row.row_value);
                query_string.push_str(", ");
            }
            query_string.pop();
            query_string.pop();
        } else {
            //ony one value to enter
            query_string.push_str(&object.row_data[0].row_name);
            query_string.push_str(") VALUES (");
            query_string.push_str(&object.row_data[0].row_value);
        }
        
        query_string.push_str(");");
        query_string
    }
    // fn parse_update(object: DataSourceObject) -> String {}
    fn parse_read(&self, object: DataSourceObject) -> String {
        let mut query_string = String::from("SELECT * FROM ");
        query_string.push_str(&object.table_name);
        query_string.push_str(";");
        query_string
    }
}




