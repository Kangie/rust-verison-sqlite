use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum ArtefactType {
    InstallerMSI = 1,
    InstallerPkg = 2,
    SourceCode = 3,
}

impl TryFrom<i32> for ArtefactType {
    type Error = &'static str;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(ArtefactType::InstallerMSI),
            2 => Ok(ArtefactType::InstallerPkg),
            3 => Ok(ArtefactType::SourceCode),
            _ => Err("Invalid value for ArtefactType"),
        }
    }
}

impl From<ArtefactType> for i32 {
    fn from(artefact_type: ArtefactType) -> Self {
        artefact_type as i32
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Artefact {
    pub artefact_type: ArtefactType,
    pub url: String,
    pub hash: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ComponentTarget {
    pub name: String,
    pub url: String,
    pub hash: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Component {
    pub name: String,
    pub version: String,
    pub target: Option<Vec<ComponentTarget>>,
    pub git_commit: Option<String>,
    pub profile_complete: bool,
    pub profile_default: bool,
    pub profile_minimal: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RustVersion {
    pub version: String,
    pub release_date: String,
    pub latest_stable: bool,
    pub latest_beta: bool,
    pub latest_nightly: bool,
    pub components: Vec<Component>,
    pub profiles: Option<std::collections::HashMap<String, Vec<String>>>,
    pub renames: Option<std::collections::HashMap<String, String>>,
    pub artefacts: Option<Vec<Artefact>>,
}

pub struct RustChannelStore {
    pub stable: Option<RustVersion>,
    pub beta: Option<RustVersion>,
    pub nightly: Option<RustVersion>,
}
