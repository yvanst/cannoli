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

import htsjdk.samtools.ValidationStringency
import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.{ AlignmentDataset, AnySAMOutFormatter }
import org.bdgenomics.adam.rdd.sequence.{ FASTAInFormatter, SequenceDataset }
import org.bdgenomics.adam.sql.{ Alignment => AlignmentProduct }
import org.bdgenomics.formats.avro.Alignment
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Option => Args4jOption }

import scala.collection.mutable.ListBuffer

/**
 * Blastn wrapper as a function SequenceDataset &rarr; AlignmentDataset,
 * for use in cannoli-shell or notebooks.
 *
 * @param args Blastn function arguments.
 * @param sc Spark context.
 */
class Blastn(val args: BlastnArgs, sc: SparkContext)
    extends CannoliFn[SequenceDataset, AlignmentDataset](sc) {

  override def apply(sequences: SequenceDataset): AlignmentDataset = {
    val cmd = ListBuffer(
      "docker",
      "run",
      "--rm",
      "-i",
      "-v",
      s"${args.dbDir}:/blast/blastdb:ro",
      "ncbi/blast:2.10.0",
      "blastn",
      "-db",
      s"${args.dbName}",
      "-outfmt",
      "17 SR SQ") ++ args.otherArgs.split(" ")

    if (args.sudo) cmd.prepend("sudo")

    info(s"Piping ${sequences} to blastn with command: ${cmd}")

    implicit val tFormatter = FASTAInFormatter
    implicit val uFormatter = new AnySAMOutFormatter(
      ValidationStringency.valueOf(args.stringency))

    sequences
      .pipe[Alignment, AlignmentProduct, AlignmentDataset, FASTAInFormatter](
        cmd = cmd,
        files = Seq())
  }
}

/**
 * Blastn function arguments.
 */
class BlastnArgs extends Args4jBase {
  @Args4jOption(required = false, name = "-sudo", usage = "Run docker via sudo.")
  var sudo: Boolean = false

  @Args4jOption(
    required = true,
    name = "-db_dir",
    usage = "the directory where databases reside, should exist on local disk")
  var dbDir: String = _

  @Args4jOption(
    required = true,
    name = "-db_name",
    usage = "blast -db {}, the database name, should exist on local disk")
  var dbName: String = _

  @Args4jOption(
    required = false,
    name = "-other_args",
    usage =
      "other arguments for Blast, must be double-quoted. " +
        "e.g. -additional_args \"-num_threads 2 ...\". notice -outfmt 17 SR SQ has been provided"
  )
  var otherArgs: String = _

  @Args4jOption(
    required = false,
    name = "-stringency",
    usage =
      "Stringency level for various checks; can be SILENT, LENIENT, or STRICT. Defaults to STRICT.")
  var stringency: String = "STRICT"
}
