use anyhow::anyhow;
use anyhow::Result;
use regex::Regex;
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
        self.parse()?;
        Ok(())
    }

    fn parse(&self) -> Result<(String, Option<String>)> {
        let re = Regex::new("([AE]|[0-9]+)([a-z]+)?")?;

        if let Some(captures) = re.captures(&self.map) {
            let ord = match captures.get(1) {
                Some(v) => v.as_str(),
                None => return Err(anyhow!("Failed to parse ordering")),
            };

            let loc = captures.get(2).map(|v| v.as_str().to_string());

            if let Some(l) = loc.clone() {
                match l.as_str() {
                    "numa" | "slot" | "node" => {}
                    _ => {
                        return Err(anyhow!("No such location specifier {}", l));
                    }
                };
            }

            return Ok((ord.to_string(), loc));
        }

        Err(anyhow!("Bad syntax in {}", self.map))
    }
}

#[derive(Deserialize, Debug)]
pub(crate) struct JobList {
    jobs: Vec<Job>,
}

impl JobList {
    pub(crate) fn load(file: PathBuf) -> Result<JobList> {
        let jobs: Vec<Job> = match fs::read_to_string(file) {
            Ok(s) => match serde_yaml::from_str(&s) {
                Ok(j) => j,
                Err(e) => return Err(anyhow!(e)),
            },
            Err(e) => return Err(anyhow!(e)),
        };

        let ret = JobList { jobs };
        ret.check()
    }

    fn check(self) -> Result<JobList> {
        for j in self.jobs.iter() {
            j.check()?;
        }
        Ok(self)
    }
}
