package shenkerbfx.StreamSplitter

import groovy.util.logging.Slf4j

import picocli.CommandLine
import picocli.CommandLine.Command
import shenkers.path.PathResolver

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.util.concurrent.Callable

@Slf4j
class Application {

    def static main(args) {
        log.info "hello world"
        CommandLine commandLine = new CommandLine(new SplitCommand());
        commandLine.registerConverter(File.class, PathResolver::resolveFile);

        commandLine.setExecutionStrategy(new CommandLine.RunLast());
        int exitCode = commandLine.execute(args);

        System.exit(exitCode);
    }
}

@Slf4j
@Command(name = 'Split')
class SplitCommand implements Callable<Integer> {

    @CommandLine.Option(names=["--chunk-size", "-s"], description="number of lines in a chunk", required=true)
    Integer chunkSize;

    @CommandLine.Option(names=["--num-chunks", "-n"], description="number of chunks to split", required=true)
    Integer n;

    @CommandLine.Option(names=["--basename", "-b"], description="base-name for the generated output", required=true)
    String base = "split";

    @CommandLine.Option(names=["--gunzip-input"], description="whether the input needs to be gzip-decompressed", negatable=true)
    Boolean decompress = true;

    @CommandLine.Option(names=["--gzip-output"], description="whether the output should be gzipped", negatable=true)
    Boolean compress = true;

    @CommandLine.Parameters(arity="1", description="path specifying the file to be split. If no file is provided will read from /dev/stdin.")
    File input = null;

//			@Parameter(names="-a", description="number of zeros to pad the output", converter=IntegerConverter.class)
//			Integer a=3;

    @Override
    public Integer call() throws Exception {
        BufferedReader scan
        if (input != null) {
            // If input file is specified, use it
            InputStream inputStream = new FileInputStream(input)
            // Apply gzip decompression if requested
            if (decompress) {
                inputStream = new GzipCompressorInputStream(inputStream)
            }
            scan = new BufferedReader(new InputStreamReader(inputStream))
        } else {
            // If no input file specified, use stdin
            scan = new BufferedReader(new InputStreamReader(System.in))
        }
        BufferedWriter[] bw = new BufferedWriter[n];
        for(int i=0; i<n; i++){

            OutputStream fos = null;
            if(compress)
                fos = new GzipCompressorOutputStream(new FileOutputStream(Util.sprintf("${base}.%0${a}d.gz",i)));
            else
                fos = new FileOutputStream(Util.sprintf("${base}.%0${a}d",i));
            bw[i] = new BufferedWriter(new OutputStreamWriter(fos));
        }

        String line = scan.readLine();

        int nChunks=n;
        int chunkSize=chunkSize;
        int chunk_i=0;
        int line_i=0;
        while(line!=null){
            bw[chunk_i].write(line);
            bw[chunk_i].write('\n');

            line_i++;

            if(chunkSize==line_i){
                line_i=0;
                chunk_i++;
            }
            if(nChunks==chunk_i){
                chunk_i=0;
            }

            line = scan.readLine();
        }

        scan.close();
        for(int i=0; i<n; i++){
            bw[i].close();
        }
    }
}
