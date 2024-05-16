use anyhow::Result;
use hwlocality::cpu::cpuset::CpuSet;
use hwlocality::object::types::ObjectType;
use hwlocality::{topology::builder::BuildFlags, Topology};
use serde::Serialize;

#[derive(Serialize)]
struct JobDesc {
    host: String,
    numa: Vec<usize>,
    pu: Vec<usize>,
}

fn main() -> Result<()> {
    let topology: Topology = Topology::builder()
        .with_flags(BuildFlags::RESTRICT_CPU_TO_THIS_PROCESS | BuildFlags::ASSUME_THIS_SYSTEM)?
        .build()?;

    let numa: Vec<usize> = topology
        .objects_with_type(ObjectType::NUMANode)
        .filter(|n| !n.cpuset().unwrap().is_empty())
        .filter_map(|n| n.os_index())
        .collect();

    let pu: Vec<usize> = topology
        .objects_with_type(ObjectType::PU)
        .filter_map(|n| n.os_index())
        .collect();

    let host = hostname::get()?
        .into_string()
        .unwrap_or("unknown".to_string());

    println!(
        "{}",
        serde_json::to_string(&JobDesc { host, numa, pu }).unwrap()
    );

    Ok(())
}
