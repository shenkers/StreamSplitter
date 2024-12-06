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

    @CommandLine.Option(names=["--chunk", "-c"], description="0 based index of the chunk", required=true)
    Integer chunk;

    @CommandLine.Option(names=["--chunk-size", "-s"], description="number of lines in a chunk", required=true)
    Integer chunkSize;

    @CommandLine.Option(names=["--num-chunks", "-n"], description="number of chunks to split", required=true)
    Integer n;

    @Override
    public Integer call() throws Exception {
        BufferedReader scan = new BufferedReader(new InputStreamReader(System.in));
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out));
        String line = scan.readLine();
        int NR=1;
        int nChunks=n;
        int chunkSize=chunkSize;
        int chunk=chunk;
        while(line!=null){

            if((NR-1)%(nChunks*chunkSize)>=chunk*chunkSize && (NR-1)%(nChunks*chunkSize) < (chunk+1)*chunkSize){
                out.write(line);
                out.write('\n');
            }

            line = scan.readLine();;
            NR++;
        }

        scan.close();
        out.close();
    }
}
