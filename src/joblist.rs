use anyhow::anyhow;
use anyhow::Result;
use serde::Deserialize;
use serde_yaml;
use std::fs;
use std::path::PathBuf;

#[derive(Deserialize, Debug)]
pub(crate) struct Job {
    map: String,
    command: Vec<String>,
}

impl Job {
    fn check(&self) -> Result<()> {
        Ok(())
    }
}

pub(crate) struct JobDesc {
    jobs: Vec<Job>,
}

impl JobDesc {
    fn load(file: PathBuf) -> Result<JobDesc> {
        let jobs: Vec<Job> = match fs::read_to_string(&file) {
            Ok(s) => match serde_yaml::from_str(&s) {
                Ok(j) => j,
                Err(e) => return Err(anyhow!(e)),
            },
            Err(e) => return Err(anyhow!(e)),
        };

        Ok(JobDesc { jobs })
    }

    fn check(&self) -> Result<()> {
        for j in self.jobs.iter() {
            j.check()?;
        }
        Ok(())
    }
}
