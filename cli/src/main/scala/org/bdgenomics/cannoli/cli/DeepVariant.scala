/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.cannoli.cli

import grizzled.slf4j.Logging
import htsjdk.samtools.ValidationStringency
import org.apache.hadoop.fs.{ FileAlreadyExistsException, FileSystem, Path }
import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.adam.util.FileExtensions._
import org.bdgenomics.cannoli.{ DeepVariant => DeepVariantFn, DeepVariantArgs => DeepVariantFnArgs }
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object DeepVariant extends BDGCommandCompanion {
  val commandName = "deepvariant"
  val commandDescription = "Call variants from an alignment dataset with DeepVariant."

  def apply(cmdLine: Array[String]) = {
    new DeepVariant(Args4j[DeepVariantArgs](cmdLine))
  }
}

/**
 * DeepVariant command line arguments.
 */
class DeepVariantArgs extends DeepVariantFnArgs with ADAMSaveAnyArgs with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "Location to pipe alignment records from (e.g. .bam, .cram, .sam). If extension is not detected, Parquet is assumed.", index = 0)
  var inputPath: String = _

  @Argument(required = true, metaVar = "OUTPUT", usage = "Location to pipe variant contexts to (e.g. .vcf, .vcf.gz, .vcf.bgz). If extension is not detected, Parquet is assumed.", index = 1)
  var outputPath: String = _

  @Args4jOption(required = false, name = "-repartition", usage = "repartition the input file.")
  var repartition: Int = 500

  @Args4jOption(required = false, name = "-single", usage = "Saves OUTPUT as single file.")
  var asSingleFile: Boolean = false

  @Args4jOption(required = false, name = "-defer_merging", usage = "Defers merging single file output.")
  var deferMerging: Boolean = false

  @Args4jOption(required = false, name = "-disable_fast_concat", usage = "Disables the parallel file concatenation engine.")
  var disableFastConcat: Boolean = false

  @Args4jOption(required = false, name = "-stringency", usage = "Stringency level for various checks; can be SILENT, LENIENT, or STRICT. Defaults to STRICT.")
  var stringency: String = "STRICT"

  // must be defined due to ADAMSaveAnyArgs, but unused here
  var sortFastqOutput: Boolean = false
}

/**
 * DeepVariant command line wrapper.
 */
class DeepVariant(protected val args: DeepVariantArgs) extends BDGSparkCommand[DeepVariantArgs] with Logging {
  val companion = DeepVariant
  val stringency: ValidationStringency = ValidationStringency.valueOf(args.stringency)

  def run(sc: SparkContext) {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    if (fs.exists(new Path(args.outputPath))) {
      throw new FileAlreadyExistsException(s"${args.outputPath} already exists on HDFS")
    }

    val alignments = sc.loadAlignments(args.inputPath, stringency = stringency).transform(rdd => rdd.repartition(args.repartition))
    val variantContexts = new DeepVariantFn(args, stringency, sc).apply(alignments)

    if (isVcfExt(args.outputPath)) {
      variantContexts.saveAsVcf(
        args.outputPath,
        asSingleFile = args.asSingleFile,
        deferMerging = args.deferMerging,
        disableFastConcat = args.disableFastConcat,
        stringency
      )
    } else {
      variantContexts.saveAsParquet(args)
    }
  }
}
