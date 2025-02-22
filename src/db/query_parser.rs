pub struct RowData {
    pub row_name: String,
    pub row_value: String
}
pub struct DataSourceObject {
    pub table_name: String,
    pub row_data: Vec<RowData>
}
//Trait parse corresponds to what database we are using (i.e. Postgres, MySQL, etc)
pub trait Parse {
    fn parse_insert(&self, object: DataSourceObject) -> String;
    // fn parse_update(object: DataSourceObject) -> String;
    fn parse_read(&self, object: DataSourceObject) -> String; //atm this is read all i.e "SELECT * FROM table_name"
}

pub struct QueryParser <T: Parse> {
    parser_type: T
}

impl <T: Parse> QueryParser<T> {
    pub fn new(parser_type: T) -> Self {
        Self { parser_type }
    }

    pub fn parse_dso(&self, query_type: &str, object: DataSourceObject) -> String {
        match query_type {
            "insert" | "add" => self.parser_type.parse_insert(object),
            "read" | "show" => self.parser_type.parse_read(object),
            _ => panic!("Invalid query type")
        }
    }
}