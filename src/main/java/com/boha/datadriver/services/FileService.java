package com.boha.datadriver.services;

import com.boha.datadriver.util.E;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Service;

import java.io.File;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;

@Service
public class FileService {
    private static final Logger LOGGER = Logger.getLogger(FileService.class.getSimpleName());

    public Resource getFileResource(String filename) throws Exception {
        LOGGER.info(E.BLUE_DOT+E.BLUE_DOT+" getFileResource: ..... Loading file: " + filename);
        try {
            File mFile = new File(filename);
            if (!mFile.exists()) {
                LOGGER.info(E.RED_DOT+E.RED_DOT+" File does not exist: " + mFile.getPath());
                throw new RuntimeException(E.RED_DOT + " File "+filename+" does not exist");
            } else {
                LOGGER.info(E.BLUE_DOT+E.BLUE_DOT+E.BLUE_DOT
                        +" File found, length: " + mFile.length() + " path: " + mFile.getPath());
            }
            Path file = Paths.get(mFile.toURI());
            Resource resource = new UrlResource(file.toUri());

            if (resource.exists() || resource.isReadable()) {
                LOGGER.info(E.BLUE_DOT+E.BLUE_DOT+E.BLUE_DOT+" File resource exists and is readable: " + resource.isFile()
                        + " path: " + resource.getFile().getPath());
                return resource;
            } else {
                throw new RuntimeException(E.RED_DOT+" Could not read the file!");
            }
        } catch (MalformedURLException e) {
            throw new RuntimeException(E.RED_DOT+" Error: " + e.getMessage());
        }
    }

    public int deleteTemporaryFiles() {
        File curDir = new File(".");
        cnt = 0;
        deleteTemporaryFiles(curDir);

        return cnt;
    }
    int cnt = 0;
    private void deleteTemporaryFiles(File curDir) {
        File[] filesList = curDir.listFiles();
        assert filesList != null;

        for (File f: filesList) {
            if (f.isDirectory())
                deleteTemporaryFiles(f);
            if (f.isFile()) {
                if (f.getName().contains("events-")) {
                    boolean done = f.delete();
                    cnt++;
                    LOGGER.info(E.AMP + E.AMP + " file: " + f.getName() + " " + E.RED_DOT + " deleted: " + done);
                }
            }
        }
    }
}
