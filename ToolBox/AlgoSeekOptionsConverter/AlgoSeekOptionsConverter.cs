/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using QuantConnect.Util;

namespace QuantConnect.ToolBox.AlgoSeekOptionsConverter
{
    using Processors = ConcurrentDictionary<Symbol, List<AlgoSeekOptionsProcessor>>;

    /// <summary>
    /// Process a directory of algoseek option files into separate resolutions.
    /// </summary>
    public class AlgoSeekOptionsConverter
    {
        private string _source;
        private string _remote;
        private string _remoteMask;
        private string _destination;
        private Resolution _resolution;
        private DateTime _referenceDate;

        private readonly ParallelOptions parallelOptionsProcessing = new ParallelOptions {MaxDegreeOfParallelism = Environment.ProcessorCount * 2};

        private readonly ParallelOptions parallelOptionsZipping = new ParallelOptions {MaxDegreeOfParallelism = Environment.ProcessorCount * 10};

        /// <summary>
        /// Create a new instance of the AlgoSeekOptions Converter. Parse a single input directory into an output.
        /// </summary>
        /// <param name="resolution">Convert this resolution</param>
        /// <param name="referenceDate">Datetime to be added to the milliseconds since midnight. Algoseek data is stored in channel files (XX.bz2) and in a source directory</param>
        /// <param name="source">Remote directory of the .bz algoseek files</param>
        /// <param name="source">Source directory of the .csv algoseek files</param>
        /// <param name="destination">Data directory of LEAN</param>
        public AlgoSeekOptionsConverter(Resolution resolution, DateTime referenceDate, string remote, string remoteMask,
            string source, string destination)
        {
            _remote = remote;
            _remoteMask = remoteMask;
            _source = source;
            _referenceDate = referenceDate;
            _destination = destination;
            _resolution = resolution;
        }

        /// <summary>
        /// Give the reference date and source directory, convert the algoseek options data into n-resolutions LEAN format.
        /// </summary>
        /// <param name="symbolFilter">HashSet of symbols as string to process. *Only used for testing*</param>
        public void Convert(HashSet<string> symbolFilter = null)
        {
            //Get the list of all the files, then for each file open a separate streamer.
            var compressedRawDatafiles = Directory.EnumerateFiles(_remote, _remoteMask).Select(f => new FileInfo(f)).ToList();

            Log.Trace( $"AlgoSeekOptionsConverter.Convert(): Found {compressedRawDatafiles.Count} AlgoSeekOptionsReader for {_referenceDate:yyyy-MM-dd}." );
            var opraTemporalFolder = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            Directory.CreateDirectory(opraTemporalFolder);

            Log.Trace("AlgoSeekOptionsConverter.Convert(): Copying OPRA files locally.");
            compressedRawDatafiles = compressedRawDatafiles.Select(f => f.CopyTo(Path.Combine(opraTemporalFolder, f.Name)))
                .OrderBy(f => f.Length)
                .ToList();
            Log.Trace( $"AlgoSeekOptionsConverter.Convert(): OPRA files size for {_referenceDate:yyyy-MM-dd} is {compressedRawDatafiles.Sum(f => f.Length) / Math.Pow(1024, 3):N1} GB." ); 

            //Initialize parameters
            var totalLinesProcessed = 0L;
            var totalFiles = compressedRawDatafiles.Count;
            var totalFilesProcessed = 0;
            var start = DateTime.MinValue;
            var processors = new Processors();

            //Process each file massively in parallel.
            Parallel.ForEach( compressedRawDatafiles, parallelOptionsProcessing, rawDataFile =>
                {
                    Log.Trace("Source File :" + rawDataFile.Name);

                    // symbol filters 
                    // var symbolFilterNames = new string[] { "AAPL", "TWX", "NWSA", "FOXA", "AIG", "EGLE", "EGEC" };
                    // var symbolFilter = symbolFilterNames.SelectMany(name => new[] { name, name + "1", name + ".1" }).ToHashSet();
                    // var reader = new AlgoSeekOptionsReader(csvFile, _referenceDate, symbolFilter);
                    using (var reader = new AlgoSeekOptionsReader(rawDataFile.FullName, _referenceDate, symbolFilter))
                    {
                        if (start == DateTime.MinValue)
                        {
                            start = DateTime.Now;
                        }

                        if (reader.Current != null) // reader contains the data
                        {
                            do
                            {
                                var tick = reader.Current as Tick;
                                //Add or create the consolidator mechanism for symbol:
                                List<AlgoSeekOptionsProcessor> symbolProcessors;
                                if (!processors.TryGetValue(tick.Symbol, out symbolProcessors))
                                {
                                    symbolProcessors = new List<AlgoSeekOptionsProcessor>(3)
                                    {
                                        new AlgoSeekOptionsProcessor( tick.Symbol, _referenceDate, TickType.Trade, _resolution, _destination ),
                                        new AlgoSeekOptionsProcessor( tick.Symbol, _referenceDate, TickType.Quote, _resolution, _destination ),
                                        new AlgoSeekOptionsProcessor( tick.Symbol, _referenceDate, TickType.OpenInterest, _resolution, _destination )
                                    };
                                    processors[tick.Symbol] = symbolProcessors;
                                }

                                // Pass current tick into processor: enum 0 = trade; 1 = quote, , 2 = oi
                                symbolProcessors[(int) tick.TickType].Process(tick);
                                if (Interlocked.Increment(ref totalLinesProcessed) % 1000000m == 0)
                                {
                                    Log.Trace(
                                        "AlgoSeekOptionsConverter.Convert(): Processed {0,3}M ticks( {1}k / sec); Memory in use: {2} MB; Total progress: {3}%",
                                        Math.Round(totalLinesProcessed / 1000000m, 2),
                                        Math.Round(totalLinesProcessed / 1000L / (DateTime.Now - start).TotalSeconds),
                                        Process.GetCurrentProcess().WorkingSet64 / (1024 * 1024),
                                        100 * totalFilesProcessed / totalFiles
                                    );
                                }
                            } while (reader.MoveNext());

                            Log.Trace("AlgoSeekOptionsConverter.Convert(): Performing final flush to disk... ");
                        }

                        Log.Trace(
                            "AlgoSeekOptionsConverter.Convert(): Cleaning up extracted options file {0}",
                            rawDataFile.FullName
                        );
                    }

                    rawDataFile.Delete();
                    Log.Trace("AlgoSeekOptionsConverter.Convert(): Finished processing file: " + rawDataFile);
                    Interlocked.Increment(ref totalFilesProcessed);
                }
            );
            WriteToDisk(processors);
        }

        /// <summary>
        /// Write the processor queues to disk
        /// </summary>
        /// <param name="peekTickTime">Time of the next tick in the stream</param>
        /// <param name="step">Period between flushes to disk</param>
        /// <param name="final">Final push to disk</param>
        /// <returns></returns>
        private void WriteToDisk(Processors processors)
        {
            Flush(processors, DateTime.MaxValue, true);
            var dataByZipFile = processors.SelectMany(p => p.Value)
                .OrderBy(p => p.Symbol.Underlying.Value)
                .GroupBy(p => p.ZipPath.FullName);
            Parallel.ForEach( dataByZipFile, parallelOptionsZipping, (zipFileData, loopState) =>
                {
                    Directory.CreateDirectory(Path.GetDirectoryName(zipFileData.Key));

                    var filenamesAndData = new List<KeyValuePair<string, byte[]>>();
                    var dataByZipEntry = zipFileData.GroupBy(d => d.EntryPath);

                    foreach (var entryData in dataByZipEntry)
                    {
                        var data = entryData.SelectMany(d => d.Queue)
                            .OrderBy(b => b.Time)
                            .Select( b => LeanData.GenerateLine(b, SecurityType.Option, Resolution.Minute));
                        var bytesData = Encoding.UTF8.GetBytes(string.Join(Environment.NewLine, data));
                        filenamesAndData.Add(new KeyValuePair<string, byte[]>(entryData.Key.Name, bytesData));
                    }
                    Compression.ZipData(zipFileData.Key, filenamesAndData);
                    Log.Trace($"AlgoSeekOptionsConverter.WriteToDisk(): {zipFileData.Key} saved!");
                }
            );
        }

        private void Flush(Processors processors, DateTime time, bool final)
        {
            foreach (var symbol in processors.Keys)
            {
                processors[symbol].ForEach(x => x.FlushBuffer(time, final));
            }
        }

        /// <summary>
        /// Cleans zip archives and source data folders before run
        /// </summary>
        public void Clean(DateTime date)
        {
            Log.Trace(
                "AlgoSeekOptionsConverter.Clean(): cleaning all zip and csv files for {0} before start...",
                date.ToShortDateString()
            );
            var extensions = new HashSet<string> {".zip", ".csv"};
            var destination = Path.Combine(_destination, "option");
            Directory.CreateDirectory(destination);
            var dateMask = date.ToString(DateFormat.EightCharacter);
            var files = Directory.EnumerateFiles(destination, dateMask + "_" + "*.*", SearchOption.AllDirectories)
                .Where(x => extensions.Contains(Path.GetExtension(x))).ToList();
            Log.Trace("AlgoSeekOptionsConverter.Clean(): found {0} files..", files.Count);

            //Clean each file massively in parallel.
            Parallel.ForEach(
                files,
                parallelOptionsZipping,
                file =>
                {
                    try
                    {
                        File.Delete(file);
                    }
                    catch (Exception err)
                    {
                        Log.Error("AlgoSeekOptionsConverter.Clean(): File.Delete returned error: " + err.Message);
                    }
                }
            );
        }
    }
}
