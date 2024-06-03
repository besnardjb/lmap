use anyhow::anyhow;
use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;
use std::io::Write;
use std::path::PathBuf;
use std::process::Stdio;
use std::{collections::HashMap, env, io::Read};
use yansi::Paint;

use crate::JobList;

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct JobDesc {
    pub(crate) host: String,
    pub(crate) rank: u32,
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
    job: Option<u32>,
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

    fn is_free(&self) -> bool {
        self.job.is_none()
    }

    fn acquire(&mut self, jobid: u32) -> Result<()> {
        if self.job.is_some() {
            return Err(anyhow!("Job is already taken"));
        }

        self.job = Some(jobid);

        Ok(())
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
    fn count_by_rank(&self) -> Vec<((i32, Option<u32>), i32)> {
        let mut by_rank: Vec<((i32, Option<u32>), i32)> = Vec::new();

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

    fn acquire(&mut self, jobid: u32) -> Result<()> {
        for slot in self.slots.iter_mut() {
            if slot.is_free() {
                slot.acquire(jobid)?;
                return Ok(());
            }
        }
        /* If we are here we found no slot */
        Err(anyhow!("No free slot on numa {}", self.id))
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

impl Node {
    fn acquire(&mut self, jobid: u32) -> Result<()> {
        for numa in self.numas.values_mut() {
            if let Ok(()) = numa.acquire(jobid) {
                /* Stop on first numa acquire */
                return Ok(());
            }
        }
        /* If we are here we found no slot */
        Err(anyhow!("No free slot on node {}", self.host))
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
                (255, 192, 203),
                (153, 204, 255),
                (245, 222, 179),
                (170, 140, 180),
                (224, 102, 102),
                (242, 177, 155),
                (136, 160, 200),
                (226, 184, 151),
                (139, 65, 139),
                (220, 133, 123),
                (230, 185, 156),
                (154, 114, 153),
                (243, 190, 178),
                (173, 143, 162),
                (221, 105, 104),
                (234, 193, 164),
                (146, 103, 136),
                (235, 194, 163),
                (169, 134, 149),
                (227, 145, 142),
                (242, 206, 175),
                (153, 93, 93),
                (239, 180, 160),
                (156, 115, 131),
                (244, 207, 187),
                (174, 144, 154),
                (229, 166, 158),
                (246, 216, 202),
                (162, 121, 132),
                (245, 219, 204),
                (188, 155, 158),
                (234, 223, 209),
                (163, 123, 133),
                (243, 225, 203),
                (177, 147, 153),
                (230, 224, 208),
                (247, 236, 227),
                (166, 126, 136),
                (248, 235, 231),
                (194, 165, 164),
                (238, 224, 213),
                (173, 139, 143),
                (245, 232, 217),
                (201, 171, 170),
                (242, 246, 244),
                (182, 153, 158),
                (239, 243, 240),
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
        let u: usize = (i as usize) % self.cols.len();
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
                    rank: job.rank as i32,
                    pu: slots.clone(),
                    job: None,
                });
            }
        }

        Ok(ret)
    }

    pub(crate) fn to_slurm(&mut self, out: PathBuf, jobs: &JobList) -> Result<()> {
        let mut per_job: HashMap<u32, Vec<i32>> = HashMap::new();

        for slot in self.each_slot() {
            if let Some(j) = slot.job {
                let vec = per_job.entry(j).or_insert(Vec::new());
                vec.push(slot.rank);
            }
        }

        /* At this point for each job we have a rank list */
        let file = std::fs::File::create(out)?;
        let mut out = std::io::BufWriter::new(file);

        for (k, v) in per_job.iter() {
            let j = if let Some(j) = jobs.job_by_id(*k) {
                j
            } else {
                return Err(anyhow!("No such job {}", k));
            };

            let line = format!(
                "{} {}\n",
                v.iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
                j.command.join(" ")
            );

            out.write_all(line.as_bytes())?;
        }

        Ok(())
    }

    fn each_node(&mut self) -> impl Iterator<Item = &mut Node> {
        self.nodes.iter_mut().map(|(_, node)| node)
    }

    fn each_numa(&mut self) -> impl Iterator<Item = &mut Numa> {
        self.nodes
            .values_mut()
            .flat_map(|node| node.numas.values_mut())
    }

    fn each_slot(&mut self) -> impl Iterator<Item = &mut Slot> {
        self.nodes.values_mut().flat_map(|node| {
            node.numas
                .values_mut()
                .flat_map(|numa| numa.slots.iter_mut())
        })
    }

    fn count_free_slots(&mut self) -> usize {
        self.each_slot().filter(|v| v.is_free()).count()
    }

    fn map_for_defined_size(
        &mut self,
        level: &str,
        size: usize,
        jobid: u32,
        on_each: bool,
    ) -> Result<()> {
        let mut number_to_alloc = size;

        match level {
            "numa" => {
                while 0 < number_to_alloc {
                    let mut did_alloc = false;
                    for nu in self.each_numa() {
                        match nu.acquire(jobid) {
                            Ok(_) => {
                                did_alloc = true;
                                number_to_alloc -= 1;
                            }
                            Err(e) => {
                                if on_each {
                                    return Err(anyhow!("No room on Numa"));
                                }
                                continue;
                            }
                        }
                        if number_to_alloc == 0 {
                            break;
                        }
                    }

                    if did_alloc == false {
                        return Err(anyhow!(
                            "No room to alocate fixed on NUMA ({} left)",
                            number_to_alloc
                        ));
                    }
                }
            }
            "node" => {
                while 0 < number_to_alloc {
                    let mut did_alloc = false;
                    for n in self.each_node() {
                        match n.acquire(jobid) {
                            Ok(_) => {
                                did_alloc = true;
                                number_to_alloc -= 1;
                            }
                            Err(e) => {
                                if on_each {
                                    return Err(anyhow!("No room on Node"));
                                }
                                continue;
                            }
                        }
                        if number_to_alloc == 0 {
                            break;
                        }
                    }

                    if did_alloc == false {
                        return Err(anyhow!(
                            "No room to alocate fixed on NODE ({} left)",
                            number_to_alloc
                        ));
                    }
                }
            }
            "slot" => {
                for s in self.each_slot() {
                    if s.is_free() {
                        s.acquire(jobid)?;
                        number_to_alloc -= 1;
                    }

                    if number_to_alloc == 0 {
                        break;
                    }
                }

                if number_to_alloc != 0 {
                    /* Not enough slots */
                    return Err(anyhow!(
                        "Not enough slots available to allocate {} slots",
                        size
                    ));
                }
            }
            _ => {
                return Err(anyhow!("No such locality specifier {}", level));
            }
        };

        Ok(())
    }

    pub(crate) fn map(&mut self, jobs: &mut JobList) -> Result<()> {
        /* We start by mapping "for each" jobs */
        for j in jobs.each_jobs() {
            if let Some(loc) = j.loc.as_ref() {
                match loc.as_str() {
                    "numa" => {
                        for nu in self.each_numa() {
                            nu.acquire(jobs.job_id(j)?)?;
                        }
                    }
                    "node" => {
                        for n in self.each_node() {
                            n.acquire(jobs.job_id(j)?)?;
                        }
                    }
                    "slot" => {
                        for s in self.each_slot() {
                            s.acquire(jobs.job_id(j)?)?;
                        }
                    }
                    _ => {
                        return Err(anyhow!("No such locality specifier {}", loc));
                    }
                };
            } else {
                unreachable!("All Each specifier has to provide a locality");
            }
        }

        /* Now we map fixed JOBs */
        for j in jobs.fixed_jobs() {
            let number_to_alloc = match j.order.parse::<usize>() {
                Ok(num) => num,
                Err(e) => {
                    return Err(anyhow!(
                        "Failed to parse value for fixed alloc {} : {}",
                        j.order,
                        e
                    ));
                }
            };

            /* Now we want to acquire as many as per fixed using the correct walk logic */
            self.map_for_defined_size(&j.loc_or_slot(), number_to_alloc, jobs.job_id(j)?, true)?;
        }

        /* Eventually we map the "all" jobs */
        let remaining_slots = self.count_free_slots();
        let all_job_count = jobs.all_jobs_count();
        let quantum = remaining_slots / all_job_count;
        let rest = remaining_slots - (all_job_count * quantum);

        let mut is_first = true;

        for j in jobs.all_jobs() {
            /* We give the rest to the first job */
            let tsize = if is_first {
                is_first = false;
                quantum + rest
            } else {
                quantum
            };

            self.map_for_defined_size(&j.loc_or_slot(), tsize, jobs.job_id(j)?, false)?;
        }

        Ok(())
    }

    fn print_block_color(names: Vec<String>, len: usize, col: (u8, u8, u8), multiplier: u8) {
        if len == 0 {
            return;
        }

        let len = len * multiplier as usize;

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

    fn print_block(names: Vec<String>, len: usize, col: &mut RandomColor, multiplier: u8) {
        ProcMap::print_block_color(names, len, col.next(), multiplier);
    }

    pub(crate) fn display(&self) {
        let mut col = RandomColor::init();

        let multiplier = match self.count() {
            0..=10 => 10,
            11..=20 => 8,
            21..=40 => 4,
            _ => 1,
        };

        ProcMap::print_block(
            vec!["Whole System".to_string(), "System".to_string()],
            self.count() as usize,
            &mut col,
            multiplier,
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
                multiplier,
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
                    multiplier,
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
                    if let Some(job) = job {
                        ProcMap::print_block_color(
                            vec![
                                format!("Rank {} Job {}", rank, job),
                                format!("R:{} J: {}", rank, job),
                                format!("R{}J{}", rank, job),
                                format!("{}", job),
                            ],
                            count as usize,
                            col.id(job as u32),
                            multiplier,
                        );
                    } else {
                        ProcMap::print_block_color(
                            vec![format!("Rank {}", rank), format!("{}", rank)],
                            count as usize,
                            (155, 155, 155),
                            multiplier,
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
