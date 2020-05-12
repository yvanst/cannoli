#!/usr/bin/env python3

import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
import logging
import argparse


work_dir = Path(f"/tmp/deepvariant_{os.getpid()}")
cpu_num = len(os.sched_getaffinity(0)) - 1
work_dir.mkdir(parents=True, exist_ok=True)
work_dir.chmod(0o777)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
log_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.DEBUG)
stderr_handler.setFormatter(log_formatter)
logger.addHandler(stderr_handler)

logger.info("=" * 20 + "START PROGRAM" + "=" * 20)

parser = argparse.ArgumentParser(
    description="deepvariant wrapper, receive reads from stdin, pipe output_vcf to stdout"
)
parser.add_argument(
    "sudo", type=str, help="run docker via sudo, true or false",
)
parser.add_argument(
    "use_gpu",
    type=str,
    help="use the docker image that utilizing GPU, docker version above 19.03 is required; or CPU if false.\n"
    + "see https://wiki.archlinux.org/index.php/Docker#Run_GPU_accelerated_Docker_containers_with_NVIDIA_GPUs"
    + " for more information.\n use true or false",
)
parser.add_argument(
    "ref_dir",
    type=str,
    help="the reference directory that needed to be mounted as /refDir/ by docker, should exist on local disk",
)
parser.add_argument(
    "deepvariant_args",
    type=str,
    help='other arguments that needed to run deepvariant, should be quoted as one, e.g. "--ref=/refDir/refName ..." ',
)
args = parser.parse_args()
use_gpu = True if args.use_gpu.upper() == "TRUE" else False
deepvariant_args = shlex.split(args.deepvariant_args)
logger.debug("deepvariant args is " + str(args.deepvariant_args))


if (work_dir / "result.vcf").exists():
    (work_dir / "result.vcf").unlink()
(work_dir / "partition.bam").write_bytes(sys.stdin.buffer.read())
logger.info("partition.bam is created")
logger.info(
    f"partition size: {(work_dir / 'partition.bam').stat().st_size / (2**20)} MB"
)

samtools_cmd = [
    "docker",
    "run",
    "--rm",
    "-i",
    "-v",
    f"{str(work_dir)}:/data",
    "biocontainers/samtools:v1.9-4-deb_cv1",
    "samtools",
    "index",
    "-b",
    "-@",
    f"{cpu_num}",
    "/data/partition.bam",
]
if args.sudo.upper() == "TRUE":
    samtools_cmd.insert(0, "sudo")
logger.debug(" ".join(samtools_cmd))

logger.info("=" * 20 + "START SAMTOOLS" + "=" * 20)
try:
    complete_process = subprocess.run(
        samtools_cmd,
        stdout=stderr_handler.stream,
        stderr=subprocess.STDOUT,
        check=True,
    )
except subprocess.CalledProcessError as e:
    logger.exception("samtools error \n")
    exit(1)
logger.info("=" * 20 + "END SAMTOOLS" + "=" * 20)


deepvariant_cmd = ["docker", "run", "--rm", "-i"]

if use_gpu:
    deepvariant_cmd.append("--gpus")
    deepvariant_cmd.append("all")
deepvariant_cmd += [
    "-v",
    f"{str(work_dir)}:/input",
    "-v",
    f"{str(work_dir)}:/output",
    "-v",
    f"{str(args.ref_dir)}:/refDir",
    f"google/deepvariant:0.10.0{'-gpu' if use_gpu else ''}",
    "/opt/deepvariant/bin/run_deepvariant",
    "--reads=/input/partition.bam",
    "--output_vcf=/output/result.vcf",
    *deepvariant_args,
]
if args.sudo.upper() == "TRUE":
    deepvariant_cmd.insert(0, "sudo")
logger.debug(" ".join(deepvariant_cmd))

logger.info("=" * 20 + "START DEEPVARIANT" + "=" * 20)
try:
    complete_process = subprocess.run(
        deepvariant_cmd,
        stdout=stderr_handler.stream,
        stderr=subprocess.STDOUT,
        check=True,
    )
except subprocess.CalledProcessError as e:
    logger.exception("deepvariant error\n")
    exit(1)
logger.info("=" * 20 + "END DEEPVARIANT" + "=" * 20)


with (work_dir / "result.vcf").open() as f:
    shutil.copyfileobj(f, sys.stdout)


logger.info("=" * 20 + "EXIT PROGRAM" + "=" * 20)
