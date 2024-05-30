use anyhow::anyhow;
use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;
use std::process::Stdio;
use std::{collections::HashMap, env, io::Read};
use yansi::Paint;

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct JobDesc {
    pub(crate) host: String,
    pub(crate) rank: i32,
    pub(crate) numa: Vec<usize>,
    pub(crate) pu: Vec<Vec<usize>>,
}

pub trait CountChild {
    fn count(&self) -> i32;
}

#[derive(Debug)]
struct Slot {
    rank: i32,
    pu: Vec<usize>,
    job: i32,
}

impl std::fmt::Display for Slot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let prettypu = self.range();
        write!(f, "Slot {{ rank: {}, pu:{} }}", self.rank, prettypu)?;
        Ok(())
    }
}

impl CountChild for Slot {
    fn count(&self) -> i32 {
        1
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

impl CountChild for Numa {
    fn count(&self) -> i32 {
        self.slots.iter().map(|v| v.count()).sum()
    }
}

impl Numa {
    fn count_by_rank(&self) -> Vec<((i32, i32), i32)> {
        let mut by_rank: Vec<((i32, i32), i32)> = Vec::new();

        /* Gather Slots by RANK */
        for slot in self.slots.iter() {
            let mut seen = false;

            for ((rank, _), val) in by_rank.iter_mut() {
                if slot.rank == *rank {
                    *val += 1;
                    seen = true;
                    break;
                }
            }

            if !seen {
                by_rank.push(((slot.rank, slot.job), 1));
            }
        }

        by_rank
    }
}

#[derive(Debug)]
struct Node {
    host: String,
    numas: HashMap<usize, Numa>,
}

impl CountChild for Node {
    fn count(&self) -> i32 {
        self.numas.iter().map(|v| v.1.count()).sum()
    }
}

struct RandomColor {
    cols: Vec<(u8, u8, u8)>,
    cur: usize,
}

impl RandomColor {
    fn init() -> RandomColor {
        RandomColor {
            cur: 0,
            cols: vec![
                (153, 204, 255),
                (112, 169, 223),
                (244, 224, 184),
                (178, 134, 202),
                (220, 208, 152),
                (136, 118, 196),
                (242, 216, 168),
                (162, 138, 204),
                (226, 210, 156),
                (144, 124, 180),
                (198, 67, 109),
                (45, 155, 235),
                (231, 156, 34),
                (118, 82, 174),
                (237, 125, 52),
                (102, 221, 135),
                (144, 51, 199),
                (255, 0, 153),
                (93, 188, 223),
                (247, 166, 39),
                (56, 114, 234),
                (165, 77, 34),
                (229, 173, 108),
                (139, 64, 217),
                (252, 227, 36),
                (81, 140, 216),
                (221, 144, 76),
                (146, 242, 134),
                (253, 215, 154),
                (69, 184, 231),
                (238, 136, 96),
                (183, 122, 223),
                (236, 204, 114),
                (115, 165, 243),
                (250, 187, 123),
                (93, 230, 155),
                (240, 174, 144),
            ],
        }
    }

    fn next(&mut self) -> (u8, u8, u8) {
        match self.cols.get(self.cur) {
            Some(c) => {
                self.cur += 1;
                *c
            }
            None => (0, 0, 0),
        }
    }

    fn id(&self, i: u32) -> (u8, u8, u8) {
        let u: usize = self.cols.len() % i as usize;
        self.cols[u]
    }
}

#[derive(Debug)]
pub(crate) struct ProcMap {
    nodes: HashMap<String, Node>,
}

impl CountChild for ProcMap {
    fn count(&self) -> i32 {
        self.nodes.iter().map(|v| v.1.count()).sum()
    }
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
                    job: -1,
                });
            }
        }

        Ok(ret)
    }

    fn print_block_color(names: Vec<String>, len: usize, col: (u8, u8, u8)) {
        if len == 0 {
            return;
        }
        let mut text: Option<String> = None;

        for s in names {
            if s.len() < len {
                text = Some(s);
                break;
            }
        }

        let mut left = len;

        if let Some(s) = text {
            print!("{}", s.black().on_rgb(col.0, col.1, col.2));
            left -= s.len();
        }

        for _ in 0..left {
            print!("{}", " ".on_rgb(col.0, col.1, col.2));
        }
    }

    fn print_block(names: Vec<String>, len: usize, col: &mut RandomColor) {
        ProcMap::print_block_color(names, len, col.next());
    }

    pub(crate) fn display(&self) {
        let mut col = RandomColor::init();

        ProcMap::print_block(
            vec!["Whole System".to_string(), "System".to_string()],
            self.count() as usize,
            &mut col,
        );

        println!();

        for (cnt, (host, node)) in self.nodes.iter().enumerate() {
            ProcMap::print_block(
                vec![
                    format!("Node {} : {}", cnt, host),
                    host.clone(),
                    format!("{}", cnt),
                ],
                node.count() as usize,
                &mut col,
            );
        }

        println!();

        /* NUMA */
        for (_, node) in self.nodes.iter() {
            for (_, numa) in node.numas.iter() {
                ProcMap::print_block(
                    vec![format!("NUMA {}", numa.id), format!("{}", numa.id)],
                    numa.count() as usize,
                    &mut col,
                );
            }
        }

        println!();

        /* SLOT */
        for (_, node) in self.nodes.iter() {
            for (_, numa) in node.numas.iter() {
                /* Print the slots */
                let by_rank = numa.count_by_rank();
                for ((rank, job), count) in by_rank {
                    if job >= 0 {
                        ProcMap::print_block_color(
                            vec![
                                format!("Rank {} Job {}", rank, job),
                                format!("R:{} J: {}", rank, job),
                                format!("{}/{}", rank, job),
                                format!("{}", job),
                            ],
                            count as usize,
                            col.id(job as u32),
                        );
                    } else {
                        ProcMap::print_block_color(
                            vec![format!("Rank {}", rank), format!("{}", rank)],
                            count as usize,
                            (155, 155, 155),
                        );
                    }
                }
            }
        }

        println!();
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
