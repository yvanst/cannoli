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

import htsjdk.samtools.ValidationStringency
import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.{ AlignmentDataset, AnySAMOutFormatter }
import org.bdgenomics.adam.rdd.sequence.{ FASTAInFormatter, SequenceDataset }
import org.bdgenomics.adam.sql.{ Alignment => AlignmentProduct }
import org.bdgenomics.cannoli.builder.CommandBuilders
import org.bdgenomics.formats.avro.Alignment
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Option => Args4jOption }

import scala.collection.JavaConversions._

/**
 * Blastp function arguments.
 */
class BlastpArgs extends Args4jBase {
  @Args4jOption(required = false, name = "-executable", usage = "Path to the blastp executable. Defaults to blastp.")
  var executable: String = "blastp"

  @Args4jOption(required = false, name = "-image", usage = "Container image to use. Defaults to quay.io/biocontainers/blast:2.9.0--pl526h3066fca_4.")
  var image: String = "quay.io/biocontainers/blast:2.9.0--pl526h3066fca_4"

  @Args4jOption(required = false, name = "-sudo", usage = "Run via sudo.")
  var sudo: Boolean = false

  @Args4jOption(required = false, name = "-add_files", usage = "If true, use the SparkFiles mechanism to distribute files to executors.")
  var addFiles: Boolean = false

  @Args4jOption(required = false, name = "-use_docker", usage = "If true, uses Docker to launch blastp.")
  var useDocker: Boolean = false

  @Args4jOption(required = false, name = "-use_singularity", usage = "If true, uses Singularity to launch blastp.")
  var useSingularity: Boolean = false

  @Args4jOption(required = true, name = "-db", usage = "BLAST database name.")
  var db: String = null

  @Args4jOption(required = false, name = "-other_args", usage = "other arguments for Blast, must be double-quoted, e.g. -additional_args \"-num_threads 2 \"")
  var otherArgs: String = _

  @Args4jOption(required = false, name = "-stringency", usage = "Stringency level for various checks; can be SILENT, LENIENT, or STRICT. Defaults to STRICT.")
  var stringency: String = "STRICT"
}

/**
 * Blastp wrapper as a function SequenceDataset &rarr; AlignmentDataset,
 * for use in cannoli-shell or notebooks.
 *
 * @param args Blastp function arguments.
 * @param sc Spark context.
 */
class Blastp(
    val args: BlastpArgs,
    sc: SparkContext) extends CannoliFn[SequenceDataset, AlignmentDataset](sc) {

  override def apply(sequences: SequenceDataset): AlignmentDataset = {

    // fail fast if db not found
    try {
      absolute(args.db)
    } catch {
      case e: FileNotFoundException => {
        error("BLAST database %s not found, touch an empty file at this path if it does not exist".format(args.db))
        throw e
      }
    }

    val builder = CommandBuilders.create(args.useDocker, args.useSingularity)
      .setExecutable(args.executable)
      .add("-db")
      .add(if (args.addFiles) "$0" else absolute(args.db))
      .add("-outfmt")
      .add("17 SR SQ")
      .add("-parse_deflines")

    if (args.addFiles) {
      // add args.db for "$0"
      builder.addFile(args.db)
      // add BLAST database files via globbed db path
      builder.addFiles(files(args.db + "*"))
    }

    if (args.useDocker || args.useSingularity) {
      builder
        .setImage(args.image)
        .setSudo(args.sudo)
        .addMount(if (args.addFiles) "$root" else root(args.db))
    }

    info("Piping %s to blastp with command: %s files: %s".format(
      sequences, builder.build(), builder.getFiles()))

    implicit val tFormatter = FASTAInFormatter
    implicit val uFormatter = new AnySAMOutFormatter(ValidationStringency.valueOf(args.stringency))

    sequences.pipe[Alignment, AlignmentProduct, AlignmentDataset, FASTAInFormatter](
      cmd = builder.build(),
      files = builder.getFiles()
    )
  }
}
