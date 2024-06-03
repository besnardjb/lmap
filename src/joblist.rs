use anyhow::anyhow;
use anyhow::Result;
use regex::Regex;
use serde::Deserialize;
use serde_yaml;
use std::fs;
use std::path::PathBuf;

#[derive(Deserialize, Debug)]
struct Job {
    map: String,
    command: Vec<String>,
}

impl Job {
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

            if ord == "E" {
                if loc.is_none() {
                    return Err(anyhow!("E job specifier requires a locality specifier"));
                }
            }

            return Ok((ord.to_string(), loc));
        }

        Err(anyhow!("Bad syntax in {}", self.map))
    }
}

#[derive(Debug)]
pub(crate) struct JobEntry {
    pub(crate) map: String,
    pub(crate) order: String,
    pub(crate) loc: Option<String>,
    pub(crate) command: Vec<String>,
}

impl JobEntry {
    fn from_job(job: Job) -> Result<JobEntry> {
        let parsed_map = job.parse()?;
        Ok(JobEntry {
            map: job.map,
            order: parsed_map.0,
            loc: parsed_map.1,
            command: job.command,
        })
    }

    pub(crate) fn loc_or_slot(&self) -> String {
        let loc = if let Some(loc) = self.loc.as_ref() {
            loc.clone()
        } else {
            "slot".to_string()
        };
        loc
    }
}

#[derive(Debug)]
pub(crate) struct JobList {
    jobs: Vec<JobEntry>,
}

impl JobList {
    pub(crate) fn load(file: PathBuf) -> Result<JobList> {
        let deserialized_jobs: Vec<Job> = match fs::read_to_string(file) {
            Ok(s) => match serde_yaml::from_str(&s) {
                Ok(j) => j,
                Err(e) => return Err(anyhow!(e)),
            },
            Err(e) => return Err(anyhow!(e)),
        };

        let mut jobs: Vec<JobEntry> = Vec::new();

        for j in deserialized_jobs {
            jobs.push(JobEntry::from_job(j)?);
        }

        Ok(JobList { jobs })
    }

    pub(crate) fn job_id(&self, job: &JobEntry) -> Result<u32> {
        for (id, j) in self.jobs.iter().enumerate() {
            if std::ptr::eq(j, job) {
                return Ok(id as u32);
            }
        }

        Err(anyhow!("No such job in list"))
    }

    pub(crate) fn job_by_id(&self, id: u32) -> Option<&JobEntry> {
        self.jobs.get(id as usize)
    }

    pub(crate) fn fixed_jobs(&self) -> impl Iterator<Item = &JobEntry> {
        self.jobs
            .iter()
            .filter(|v| (v.order != "A") && (v.order != "E"))
    }

    pub(crate) fn all_jobs(&self) -> impl Iterator<Item = &JobEntry> {
        self.jobs.iter().filter(|v| (v.order == "A"))
    }

    pub(crate) fn all_jobs_count(&self) -> usize {
        self.all_jobs().count()
    }

    pub(crate) fn each_jobs(&self) -> impl Iterator<Item = &JobEntry> {
        self.jobs.iter().filter(|v| (v.order == "E"))
    }
}
