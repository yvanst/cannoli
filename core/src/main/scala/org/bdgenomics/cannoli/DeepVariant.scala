package org.bdgenomics.cannoli

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.vcf.VCFHeaderLine
import org.apache.spark.SparkContext
import org.apache.spark.util.CollectionAccumulator
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.{ AlignmentDataset, BAMInFormatter }
import org.bdgenomics.adam.rdd.variant.{ VariantContextDataset, VCFOutFormatter }
import org.bdgenomics.adam.sql.{ VariantContext => VariantContextProduct }
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Option => Args4jOption }
import scala.collection.JavaConversions._

class DeepVariant(
    val args: DeepVariantArgs,
    val stringency: ValidationStringency = ValidationStringency.LENIENT,
    sc: SparkContext) extends CannoliFn[AlignmentDataset, VariantContextDataset](sc) {
  override def apply(alignments: AlignmentDataset): VariantContextDataset = {
    val accumulator: CollectionAccumulator[VCFHeaderLine] =
      sc.collectionAccumulator("headerLines")

    implicit val tFormatter = BAMInFormatter
    implicit val uFormatter =
      VCFOutFormatter(sc.hadoopConfiguration, stringency, Some(accumulator))

    val deepvariant_arg = Seq(
      s"--model_type=${args.modelType}",
      s"--ref=/refDir/${args.refName}",
      args.otherArgs
    ).mkString(" ")

    val variantContexts = alignments.pipe[VariantContext, VariantContextProduct, VariantContextDataset, BAMInFormatter](
      cmd = Seq(
        args.executable,
        if (args.useGPU) "true" else "false",
        args.refDir,
        deepvariant_arg
      ),
      files = Seq()
    )

    variantContexts.rdd.count()

    val headerLines = accumulator.value.distinct
    variantContexts.replaceHeaderLines(headerLines)

  }

}

class DeepVariantArgs extends Args4jBase {

  @Args4jOption(
    required = false,
    name = "-executable",
    usage =
      "Path to the deepvariantwrapper.py executable. Defaults to deepvariantwrapper.py."
  )
  var executable: String = "deepvariantwrapper.py"

  @Args4jOption(
    required = false,
    name = "-use_gpu",
    usage = "use GPU to run deepvariant, instead of using CPU"
  )
  var useGPU: Boolean = false

  @Args4jOption(
    required = true,
    name = "-ref_dir",
    usage = "run_deepvariant --ref={}, the directory part, should exist on local disk"
  )
  var refDir: String = _

  @Args4jOption(
    required = true,
    name = "-ref_name",
    usage = "run_deepvariant --ref={}, the name part, should exist on local disk"
  )
  var refName: String = _

  @Args4jOption(
    required = true,
    name = "-model_type",
    usage = "run_deepvariant --model_type={}, WGS, WES or PACBIO"
  )
  var modelType: String = _

  @Args4jOption(
    required = false,
    name = "-other_args",
    usage =
      "Other arguments for DeepVariant, must be double-quoted," +
        "e.g. -other_args \"--regions='chr20:10,000,000-10,000,100' --num_shards=4\" "
  )
  var otherArgs: String = _

}
