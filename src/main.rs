use std::ops::Deref;

use anyhow::anyhow;
use anyhow::Result;
use clap::Parser;
use hwlocality::bitmap::BitmapRef;
use hwlocality::cpu::cpuset::CpuSet;
use hwlocality::object::types::ObjectType;
use hwlocality::{topology::builder::BuildFlags, Topology};
use which::which;

mod map;
use map::JobDesc;
use map::ProcMap;

fn output_map() -> Result<()> {
    let topology: Topology = Topology::builder()
        .with_flags(BuildFlags::RESTRICT_CPU_TO_THIS_PROCESS | BuildFlags::ASSUME_THIS_SYSTEM)?
        .build()?;

    let mut per_numa_cpuset: Vec<Option<BitmapRef<CpuSet>>> = Vec::new();

    let numa: Vec<usize> = topology
        .objects_with_type(ObjectType::NUMANode)
        .map(|n| {
            per_numa_cpuset.push(n.cpuset());
            n
        })
        .filter(|n| !n.cpuset().unwrap().is_empty())
        .filter_map(|n| n.os_index())
        .collect();

    let pu = per_numa_cpuset
        .iter()
        .filter_map(|v| v.as_ref().cloned())
        .map(|v| {
            let ret = v
                .iter_set()
                .map(|v| usize::try_from(v).unwrap())
                .collect::<Vec<_>>();
            ret
        })
        .collect::<Vec<_>>();

    let host = hostname::get()?
        .into_string()
        .unwrap_or("unknown".to_string());

    let rank: i32 = match std::env::var("PMI_RANK") {
        Ok(val) => val.parse().unwrap(),
        Err(_) => match std::env::var("PMIX_RANK") {
            Ok(val) => val.parse().unwrap(),
            Err(_) => -1,
        },
    };

    println!(
        "{}",
        serde_json::to_string(&JobDesc {
            host,
            rank,
            numa,
            pu
        })
        .unwrap()
    );

    Ok(())
}

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, short, action)]
    /// Output mapping information for current process
    map: bool,
    #[clap(long, short, action)]
    /// Output mapping information for current process
    display: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Bypass for use in discovery
    if args.map {
        if args.display {
            return Err(anyhow!(
                "map (-m) and display options (-d) are mutually exclusive"
            ));
        }
        return output_map();
    }

    // Make sure we have srun in path
    if let Err(e) = which("srun") {
        println!("srun not found in PATH (cannot continue");
        return Err(anyhow!(e));
    }

    let pmap = ProcMap::init()?;

    if args.display {
        println!("{}", pmap);
        return Ok(());
    }

    Ok(())
}
