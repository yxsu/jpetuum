package com.petuum.ps;

import java.nio.file.FileSystems;
import java.nio.file.Path;

/**
 * Created by suyuxin on 14-9-3.
 */
public class FileSystemTest {
    public static void main(String[] args) {
        Path path = FileSystems.getDefault().getPath("machines", "localserver");
        System.out.println(path.toAbsolutePath().toString());
    }
}
