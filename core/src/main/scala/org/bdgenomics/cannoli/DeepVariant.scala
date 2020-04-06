package org.bdgenomics.cannoli

import java.nio.file.Paths

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.vcf.VCFHeaderLine
import org.apache.spark.{SparkContext, SparkFiles}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CollectionAccumulator
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.rdd.variant.{VariantContextDataset, VCFOutFormatter}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.{AlignmentDataset, BAMInFormatter}
import org.bdgenomics.adam.sql.{VariantContext => VariantContextProduct}
import org.kohsuke.args4j.{Option => Args4jOption}

import org.bdgenomics.utils.cli._

class DeepVariant(
    val args: DeepVariantArgs,
    val stringency: ValidationStringency = ValidationStringency.LENIENT,
    sc: SparkContext
) extends CannoliFn[AlignmentDataset, VariantContextDataset](sc) {
  override def apply(alignments: AlignmentDataset): VariantContextDataset = {

    // copy associated files to every worker, use spark --files instead??
    args.associatedFiles.split(",").map(_.trim).foreach(f => sc.addFile(f))

    val accumulator: CollectionAccumulator[VCFHeaderLine] =
      sc.collectionAccumulator("headerLines")

    implicit val tFormatter = BAMInFormatter
    // TODO: return RDD[Int] as exitcode, in order to handle error??
    implicit val uFormatter =
      VCFOutFormatter(sc.hadoopConfiguration, stringency, Some(accumulator))

    // run cp using pipe, copy partition to executor's local disk
    val dummy: RDD[Int] = alignments.pipe[
      VariantContext,
      VariantContextProduct,
      VariantContextDataset,
      BAMInFormatter
    ](
      cmd = List("cp", "/dev/stdin", "$root/partition.bam"),
      files = Seq()
    )

    // TODO: error handling??

    // use samtools to generate index file
    // then run DeepVariant
    // make sure it's the same partition as before
    val variantContexts = dummy.mapPartitionsWithIndex((idx, partition) => {
      val (workDir, samtoolsCMD, deepVariantCMD) = getCMD(idx)
      val pb = sys.process.Process(samtoolsCMD) #&&
        sys.process.Process(deepVariantCMD)
      pb.!
      if (args.outputGvcf) {
        // upload to HDFS
        // ${workDir}/result-${idx}.g.vcf.gz

      }
      if (args.vcfStatsReport) {
        // upload to HDFS
        // ${workDir}/result-${idx}.visual_report.html
      }

      // TODO: read result as a partition in vcf format
      val buffer = scala.io.Source.fromFile(s"${workDir}/result-${idx}.vcf")
      val res = buffer.tovcf.toIterator
      buffer.close()
      res
    })

    // TODO: error handling?

    // TODO: headerline
    val headerLines = accumulator.value.distinct
    variantContexts.replaceHeaderLines(headerLines)
    variantContexts
  }

  def getCMD(idx: Int): (String, List[String], List[String]) = {
    val workDir =
      Paths.get(SparkFiles.getRootDirectory()).toAbsolutePath.toString

    // https://hub.docker.com/r/biocontainers/samtools/dockerfile
    val samtoolsCMD = List(
      "docker",
      "run",
      "--rm",
      "-v",
      s"${workDir}:/data",
      "biocontainers/samtools:v1.9-4-deb_cv1",
      "samtools",
      "index",
      "-b",
      "-@",
      s"${args.numShards}",
      "/data/partition.bam"
    )
    info(samtoolsCMD)

    val deepVariantCMD = collection.mutable.ListBuffer("docker", "run", "--rm")
    // https://wiki.archlinux.org/index.php/Docker#Run_GPU_accelerated_Docker_containers_with_NVIDIA_GPUs
    if (args.useGPU) deepVariantCMD.appendAll(Seq("--gpus", "all"))
    deepVariantCMD.appendAll(
      Seq(
        "-v",
        s"${workDir}:/input",
        "-v",
        s"${workDir}:/output",
        s"google/deepvariant:${args.imageVersion}${if (args.useGPU) "-gpu"
        else ""}",
        "/opt/deepvariant/bin/run_deepvariant",
        "--reads=/input/partition.bam",
        s"--output_vcf=/output/result-${idx}.vcf",
        s"--model_type=${args.modelType}",
        s"--ref=${args.ref}"
      )
    )
    // optional flags
    for ((k, v) <- List(
           args.regions -> "--regions=%s",
           args.numShards -> "--num_shards=%s",
           args.regions -> "--regions=%s",
           args.sampleName -> "--sample_name=%s",
           args.makeExamplesExtraArgs -> "--make_example_extra_args=%s",
           args.callVariantsExtraArgs -> "--call_variants_extra_args=%s",
           args.postProcessExtraArgs -> "--postprocess_variants_extra_args=%s",
           args.vcfStatsReport -> "--vcf_stats_report=%s"
         )) Option(k).foreach(a => deepVariantCMD.append(v.format(a)))

    if (args.outputGvcf)
      deepVariantCMD.append(s"--output_gvcf=/output/result-${idx}.g.vcf.gz")
    info(deepVariantCMD)
    (workDir, samtoolsCMD, deepVariantCMD.toList)
  }
}

class DeepVariantArgs extends Args4jBase {
  @Args4jOption(
    required = false, // use spark --files instead??
    name = "-associatedFiles",
    usage =
      "additional files that DeepVariant needs, separated with `,`, use full path." +
        "e.g. `/path/to/a.fasta,/path/to/a.fasta.fai`." +
        "use spark-shell/submit --files instead"
  )
  var associatedFiles: String = _

  @Args4jOption(
    required = false,
    name = "-useGPU",
    usage = "docker run google/deepvariant:latest{-gpu}, default is true"
  )
  var useGPU: Boolean = true

  @Args4jOption(
    required = false,
    name = "-imageVersion",
    usage = "docker google/deepvariant:{}-gpu, default is 0.9.0"
  )
  var imageVersion: String = "0.9.0"

  @Args4jOption(
    required = true,
    name = "-ref, should match associated files",
    usage = "run_deepvariant --ref={}"
  )
  var ref: String = _

  @Args4jOption(
    required = true,
    name = "-model_type",
    usage = "run_deepvariant --model_type={}, WGS, WES or PACBIO"
  )
  var modelType: String = _

  @Args4jOption(
    required = true,
    name = "-read",
    usage = "run_deepvariant --reads={}"
  )
  var reads: String = _

  @Args4jOption(
    required = false,
    name = "-regions",
    usage = "run_deepvariant --regions={}"
  )
  var regions: String = _

  @Args4jOption(
    required = false,
    name = "-num_shards",
    usage = "run_deepvariant --num_shards={}"
  )
  var numShards: Integer = _

  @Args4jOption(
    required = false,
    name = "-sample_name",
    usage = "run_deepvariant --sample_name={}"
  )
  var sampleName: String = _

  @Args4jOption(
    required = false,
    name = "-make_examples_extra_args",
    usage = "run_deepvariant --make_example_extra_args={}"
  )
  var makeExamplesExtraArgs: String = _

  @Args4jOption(
    required = false,
    name = "-call_variants_extra_args",
    usage = "run_deepvariant --call_variants_extra_args={}"
  )
  var callVariantsExtraArgs: String = _

  @Args4jOption(
    required = false,
    name = "-postprocess_variants_extra_args",
    usage = "run_deepvariant --postprocess_variants_extra_args={}"
  )
  var postProcessExtraArgs: String = _
  @Args4jOption(
    required = false,
    name = "-output_gvcf",
    usage = "run_deepvariant --output_gvcf, true or false, default is false"
  )
  var outputGvcf: Boolean = false

  @Args4jOption(
    required = false,
    name = "-vcf_stats_report",
    usage =
      "run_deepvariant --vcf_stats_report, true or false, default is false"
  )
  var vcfStatsReport: Boolean = false
}
