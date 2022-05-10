use std::path::Path;
use std::fmt::Debug;

pub trait VarSource {
    fn get_environment_variable<T>(&self, name: &[T]) -> Result<String, crate::Error>
    where
        T: AsRef<str> + Debug;
}

// TODO:
pub struct KeyVaultSource;

#[derive(Debug, Clone)]
pub struct EnvVarSource;

impl VarSource for EnvVarSource {
    fn get_environment_variable<T>(&self, name: &[T]) -> Result<String, crate::Error>
    where
        T: AsRef<str> + Debug,
    {
        let name: Vec<&str> = name.into_iter().map(|s| s.as_ref()).collect();
        Ok(std::env::var(name.join("__").to_uppercase())?)
    }
}

#[derive(Debug, Clone)]
pub struct YamlSource {
    root: serde_yaml::Value,
    overlay: EnvVarSource,
}

impl YamlSource {
    pub fn load<T>(config_path: T) -> Result<Self, crate::Error>
    where
        T: AsRef<Path>,
    {
        let f = std::fs::File::open(config_path)?;
        let root = serde_yaml::from_reader(f)?;
        Ok(Self {
            root,
            overlay: EnvVarSource,
        })
    }

    fn get_value_by_path<T>(
        &self,
        node: &serde_yaml::Value,
        name: &[T],
    ) -> Result<String, crate::Error>
    where
        T: AsRef<str> + Debug,
    {
        if name.is_empty() {
            // Recursion ends
            return Ok(node
                .as_str()
                .ok_or_else(|| crate::Error::InvalidConfig("Current node is not a string".to_string()))?
                .to_string());
        }
        
        let key = serde_yaml::Value::String(name[0].as_ref().to_string());

        let child = node
            .as_mapping()
            .ok_or_else(|| crate::Error::InvalidConfig(format!("Current node {} is not a mapping", name[0].as_ref())))?
            .get(&key)
            .ok_or_else(|| crate::Error::InvalidConfig(format!("Key {} is missing", name[0].as_ref())))?;
        self.get_value_by_path(child, &name[1..name.len()])
    }
}

impl VarSource for YamlSource {
    fn get_environment_variable<T>(&self, name: &[T]) -> Result<String, crate::Error>
    where
        T: AsRef<str> + Debug,
    {
        self.overlay
            .get_environment_variable(name)
            .or_else(|_| self.get_value_by_path(&self.root, name))
    }
}

#[cfg(test)]
mod tests {
    use dotenv;
    use std::sync::Once;

    use super::*;

    static INIT_ENV_LOGGER: Once = Once::new();

    fn init() {
        dotenv::dotenv().ok();
        INIT_ENV_LOGGER.call_once(|| env_logger::init());
    }

    #[test]
    fn it_works() {
        init();
        let y = YamlSource::load("../test-script/feathr_config.yaml").unwrap();
        assert_eq!(y.get_environment_variable(&["project_config", "project_name"]).unwrap(), "project_feathr_integration_test");
    }
}