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
import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.cannoli.{ Blastp => BlastpFn, BlastpArgs => BlastpFnArgs }
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object Blastp extends BDGCommandCompanion {
  val commandName = "blastp"
  val commandDescription =
    "Align protein sequences in a sequence dataset with blastp."

  def apply(cmdLine: Array[String]) = {
    new Blastp(Args4j[BlastpArgs](cmdLine))
  }
}

/**
 * Blastp command line wrapper.
 */
class Blastp(protected val args: BlastpArgs)
    extends BDGSparkCommand[BlastpArgs]
    with Logging {
  val companion = Blastp
  val stringency: ValidationStringency =
    ValidationStringency.valueOf(args.stringency)

  def run(sc: SparkContext) {
    val sequences = sc.loadProteinSequences(args.inputPath)
    val alignments = new BlastpFn(args, sc).apply(sequences)
    alignments.save(args)
  }
}

/**
 * Blastp command line arguments.
 */
class BlastpArgs extends BlastpFnArgs with ADAMSaveAnyArgs with ParquetArgs {
  @Argument(
    required = true,
    metaVar = "INPUT",
    usage =
      "Location to pipe protein sequences from (e.g. FASTA format, .fa). If extension is not detected, Parquet is assumed.",
    index = 0
  )
  var inputPath: String = _

  @Argument(
    required = true,
    metaVar = "OUTPUT",
    usage =
      "Location to pipe alignments to (e.g. .bam, .cram, .sam). If extension is not detected, Parquet is assumed.",
    index = 1
  )
  var outputPath: String = _

  @Args4jOption(
    required = false,
    name = "-single",
    usage = "Saves OUTPUT as single file."
  )
  var asSingleFile: Boolean = false

  @Args4jOption(
    required = false,
    name = "-defer_merging",
    usage = "Defers merging single file output."
  )
  var deferMerging: Boolean = false

  @Args4jOption(
    required = false,
    name = "-disable_fast_concat",
    usage = "Disables the parallel file concatenation engine."
  )
  var disableFastConcat: Boolean = false

  // must be defined due to ADAMSaveAnyArgs, but unused here
  var sortFastqOutput: Boolean = false
}