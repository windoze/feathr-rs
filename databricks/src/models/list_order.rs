/*
 * Jobs API 2.1
 *
 * The Jobs API allows you to create, edit, and delete jobs.
 *
 * The version of the OpenAPI document: 2.1
 * 
 * Generated by: https://openapi-generator.tech
 */

/// ListOrder : * `DESC`: Descending order. * `ASC`: Ascending order.

/// * `DESC`: Descending order. * `ASC`: Ascending order.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum ListOrder {
    #[serde(rename = "DESC")]
    DESC,
    #[serde(rename = "ASC")]
    ASC,

}

impl ToString for ListOrder {
    fn to_string(&self) -> String {
        match self {
            Self::DESC => String::from("DESC"),
            Self::ASC => String::from("ASC"),
        }
    }
}

impl Default for ListOrder {
    fn default() -> ListOrder {
        Self::DESC
    }
}




