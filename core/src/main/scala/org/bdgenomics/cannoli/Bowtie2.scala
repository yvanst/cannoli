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
package org.bdgenomics.cannoli

import java.io.FileNotFoundException

import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.fragment.{
  FragmentDataset,
  InterleavedFASTQInFormatter
}
import org.bdgenomics.adam.rdd.read.{ AlignmentDataset, AnySAMOutFormatter }
import org.bdgenomics.adam.sql.{ Alignment => AlignmentProduct }
import org.bdgenomics.cannoli.builder.CommandBuilders
import org.bdgenomics.formats.avro.Alignment
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Option => Args4jOption }

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
 * Bowtie 2 wrapper as a function FragmentDataset &rarr; AlignmentDataset,
 * for use in cannoli-shell or notebooks.
 *
 * @param args Bowtie 2 function arguments.
 * @param sc Spark context.
 */
class Bowtie2(val args: Bowtie2Args, sc: SparkContext)
    extends CannoliFn[FragmentDataset, AlignmentDataset](sc) {

  override def apply(fragments: FragmentDataset): AlignmentDataset = {
    val cmd = ListBuffer(
      "docker",
      "run",
      "--rm",
      "-i",
      "-v",
      s"${args.indexDir}:/bowtie2/index:ro",
      "--env",
      s"BOWTIE2_INDEXES=${args.indexDir}",
      "quay.io/biocontainers/bowtie2:2.3.5.1--py37he513fc3_0",
      "-x",
      s"${args.indexName}",
      "--interleaved",
      "-"
    ) ++ args.otherArgs.split(" ")

    if (args.sudo) cmd.prepend("sudo")

    info(
      s"Piping ${fragments} to blastn with command: ${cmd}"
    )

    implicit val tFormatter = InterleavedFASTQInFormatter
    implicit val uFormatter = new AnySAMOutFormatter

    fragments.pipe[Alignment, AlignmentProduct, AlignmentDataset, InterleavedFASTQInFormatter](
      cmd = cmd,
      files = Seq()
    )
  }
}

/**
 * Bowtie 2 function arguments.
 */
class Bowtie2Args extends Args4jBase {
  @Args4jOption(required = false, name = "-sudo", usage = "Run docker via sudo.")
  var sudo: Boolean = false

  @Args4jOption(
    required = true,
    name = "-index_dir",
    usage = "the directory where indexes reside, should exist on local disk"
  )
  var indexDir: String = _

  @Args4jOption(
    required = true,
    name = "-index_name",
    usage = "bowtie2 -x {}, the index name, should exist on local disk"
  )
  var indexName: String = _

  @Args4jOption(
    required = false,
    name = "-other_args",
    usage =
      "other arguments for Bowtie2, must be double-quoted," +
        " e.g. -bowtie2_args \"-N 1 --end-to-end\""
  )
  var otherArgs: String = _
}
