package shenkerbfx.StreamSplitter

import groovy.util.logging.Slf4j

import picocli.CommandLine
import picocli.CommandLine.Command
import shenkerbfx.path.PathResolver

import java.io.BufferedReader;
import java.io.BufferedOutputStream;
import java.io.Writer;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.CompletableFuture

@Slf4j
class Application {

    def static main(args) {
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

    @CommandLine.Option(names=["--lines-per-record", "-k"], description="number of lines in a chunk", required=true)
    Integer chunkSize;

    @CommandLine.Option(names=["--num-split", "-n"], description="number of chunks to split", required=true)
    Integer n;

    @CommandLine.Option(names=["--basename", "-b"], description="base-name for the generated output", required=true)
    String base = "split";

    @CommandLine.Option(names=["--gunzip-input"], description="whether the input needs to be gzip-decompressed", negatable=true)
    Boolean decompress;

    @CommandLine.Option(names=["--gzip-output"], description="whether the output should be gzipped", negatable=true)
    Boolean compress;

    @CommandLine.Parameters(arity="0..1", description="path specifying the file to be split. If no file is provided will read from /dev/stdin.")
    File input;


    private InputStream getInputStream() {
        if (input == null) {
            return System.in
        }
        return new FileInputStream(input)
    }

    private InputStream maybeDecompress(InputStream input) {
        if (decompress) {
            return new GzipCompressorInputStream(input)
        }
        return input
    }

    private BufferedReader createReader(InputStream input) {
        return new BufferedReader(new InputStreamReader(input))
    }

    private int calculatePadding() {
        return (int)Math.floor(Math.log10(n))
    }

    private OutputStream createOutputStream(int index) {
        int padding = calculatePadding()
        return new BufferedOutputStream(
            new FileOutputStream(String.format("${base}.%0${padding + 1}d", index))
        )
    }

    private Writer createWriter(int index) {
        return new OutputStreamWriter(createOutputStream(index))
    }

    private void compressFile(File inputFile, File outputFile) {
        inputFile.withInputStream { input ->
            outputFile.withOutputStream { output ->
                def gzipOut = new GzipCompressorOutputStream(new BufferedOutputStream(output))
                input.transferTo(gzipOut)
                gzipOut.close()
            }
        }
        inputFile.delete()
    }

    @Override
    public Integer call() throws Exception {
        // Phase 1: Split files (no compression)
        BufferedReader scan = createReader(maybeDecompress(getInputStream()))
        Writer[] bw = new Writer[n]
        for(int i = 0; i < n; i++) {
            bw[i] = createWriter(i)
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

        // Phase 2: Compression (if requested)
        if (compress) {
            ExecutorService executor = Executors.newFixedThreadPool(Runtime.runtime.availableProcessors())
            List<Future<?>> compressionTasks = []

            for(int i = 0; i < n; i++) {
                int padding = calculatePadding()
                File inputFile = new File(String.format("${base}.%0${padding + 1}d", i))
                File outputFile = new File(String.format("${base}.%0${padding + 1}d.gz", i))

                compressionTasks.add(
                    CompletableFuture.runAsync({
                        compressFile(inputFile, outputFile)
                    }, executor)
                )
            }

            compressionTasks.each { it.get() }
            executor.shutdown()
        }

        return 0
    }
}
