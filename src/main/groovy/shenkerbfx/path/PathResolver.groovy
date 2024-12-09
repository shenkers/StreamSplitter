package shenkerbfx.path

import java.io.File
import java.nio.file.Paths
import java.util.Stack
import java.util.stream.Collectors

class PathResolver {

    private static final String homeDirPattern = '^~/'
    private static final String userHomeDirPattern = '^~[^/]+'

    static File resolveFile(String filePath) {
        return new File(resolve(filePath))
    }
    
    static String resolve(String filePath) {
        return simplifyPath(resolveTilde(filePath))
    }
    
    static String resolveTilde(String filePath) {
        if(filePath.equals("~")) {
            return homeDir()
        } else
        if(filePath.matches(homeDirPattern + ".*")) {
            String replacement = homeDir() + "/"
            return filePath.replaceFirst(homeDirPattern, replacement)
        } else if(filePath.matches(userHomeDirPattern + "(/.*)?")) {
            String prefix = filePath.substring(1,filePath.indexOf('/'))
            File userHomeDir = new File(new File(homeDir()).getParentFile(),prefix)
            File withSuffix = new File(userHomeDir, filePath.substring(filePath.indexOf('/')+1))
            return withSuffix.path
        }
        return filePath
    }
    
    private static String homeDir() {
        return System.getProperty("user.home")
    }
    
    private static String simplifyPath(String path) {
        if(path.length() > 0 && path.charAt(0)=='/') {
            Stack<String> simplified = new Stack<>()
            simplified.push("")
            StringBuilder pathBuilder = new StringBuilder(path.substring(1))
            
            while(pathBuilder.length() > 0) {
                
                int indexOfSlash = pathBuilder.indexOf("/")
                if(indexOfSlash > -1) {
                    String element = pathBuilder.substring(0,indexOfSlash)
                    pathBuilder.delete(0,indexOfSlash+1)
                    updatePath(simplified,element)
                }else {
                    updatePath(simplified,pathBuilder.toString())
                    pathBuilder.setLength(0)
                }
                
            }
            
            return simplified.stream().collect(Collectors.joining("/"))
        }else {
            return path
        }
    }
    
    private static void updatePath(Stack<String> path, String element) {
        if(element.equals(".") || element.equals("")) {
            // no-op
        }else if(element.equals("../") || element.equals("..") ) {
            path.pop()
        }else {
            path.push(element)
        }
    }
}
