use anyhow::anyhow;
use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;
use std::process::Stdio;
use std::{collections::HashMap, env, io::Read};

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct JobDesc {
    pub(crate) host: String,
    pub(crate) rank: i32,
    pub(crate) numa: Vec<usize>,
    pub(crate) pu: Vec<Vec<usize>>,
}
#[derive(Debug)]
struct Slot {
    rank: i32,
    pu: Vec<usize>,
}

impl std::fmt::Display for Slot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let prettypu = self.range();
        write!(f, "Slot {{ rank: {}, pu:{} }}", self.rank, prettypu)?;
        Ok(())
    }
}

impl Slot {
    fn range(&self) -> String {
        let candi_range = if let (Some(mi), Some(ma)) = (self.pu.iter().min(), self.pu.iter().max())
        {
            (*mi..=*ma).collect()
        } else {
            vec![]
        };

        if candi_range == self.pu && (candi_range.len() > 1) {
            format!(
                "{}-{}",
                candi_range.first().unwrap_or(&0),
                candi_range.last().unwrap_or(&0)
            )
        } else {
            format!("pu: {:?}", self.pu)
        }
    }
}

#[derive(Debug)]
struct Numa {
    id: usize,
    slots: Vec<Slot>,
}
#[derive(Debug)]
struct Node {
    host: String,
    numas: HashMap<usize, Numa>,
}

#[derive(Debug)]
pub(crate) struct ProcMap {
    nodes: HashMap<String, Node>,
}

impl std::fmt::Display for ProcMap {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "ProcMap:")?;
        for (host, node) in self.nodes.iter() {
            writeln!(f, "{}:", host)?;
            for (_, numa) in node.numas.iter() {
                writeln!(f, "\tNuma ID: {}", numa.id)?;

                for slot in numa.slots.iter() {
                    writeln!(f, "\t\tSlot Rank: {} PU : {}", slot.rank, slot.range())?;
                }
            }
        }
        Ok(())
    }
}

impl ProcMap {
    pub(crate) fn init() -> Result<ProcMap> {
        let mut ret = ProcMap {
            nodes: HashMap::new(),
        };

        // Discover topology
        let jobs = ProcMap::discovery()?;

        // Insert in internal state
        for job in jobs.iter() {
            let node = ret.nodes.entry(job.host.clone()).or_insert(Node {
                host: job.host.clone(),
                numas: HashMap::new(),
            });
            for (cnt, numa_id) in job.numa.iter().enumerate() {
                let numa = node.numas.entry(*numa_id).or_insert(Numa {
                    id: *numa_id,
                    slots: Vec::new(),
                });

                let slots = job.pu.get(cnt).expect("Failed to retrieve slots");

                numa.slots.push(Slot {
                    rank: job.rank,
                    pu: slots.clone(),
                });
            }
        }

        Ok(ret)
    }

    fn discovery() -> Result<Vec<JobDesc>> {
        let self_exe = match env::current_exe() {
            Ok(e) => e,
            Err(e) => {
                return Err(anyhow!(e));
            }
        };

        let mut output = String::new();

        // Spawn the process [srun, lmap, -m]
        let mut srun = std::process::Command::new("srun")
            .arg(self_exe.to_str().unwrap())
            .arg("-m")
            .stdout(Stdio::piped())
            .spawn()?;

        srun.stdout.take().unwrap().read_to_string(&mut output)?;

        let mut ret: Vec<JobDesc> = Vec::new();

        // Capture input line by line
        for line in output.lines() {
            if let Ok(jd) = serde_json::from_str(line) {
                ret.push(jd);
            } else {
                println!("Failed to parse JobDesc : {}", line);
            }
        }

        Ok(ret)
    }
}
