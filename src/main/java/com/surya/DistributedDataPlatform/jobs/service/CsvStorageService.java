package com.surya.DistributedDataPlatform.jobs.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

@Service
public class CsvStorageService {

    private final Path uploadDirectory;

    public CsvStorageService(
            @Value("${app.upload-dir:./uploads}") String uploadDirectory) {
        this.uploadDirectory = Path.of(uploadDirectory)
                .toAbsolutePath()
                .normalize();
    }

    public String store(MultipartFile file) throws IOException {
        Files.createDirectories(uploadDirectory);

        String storedName = UUID.randomUUID() + ".csv";
        Path destination = uploadDirectory.resolve(storedName).normalize();

        if (!destination.startsWith(uploadDirectory)) {
            throw new IOException("Invalid upload destination");
        }

        Files.copy(file.getInputStream(), destination,
                StandardCopyOption.REPLACE_EXISTING);

        return destination.toString();
    }

    public void delete(String filePath) throws IOException {
        Path path = Path.of(filePath).toAbsolutePath().normalize();

        if (path.startsWith(uploadDirectory)) {
            Files.deleteIfExists(path);
        }
    }
}
