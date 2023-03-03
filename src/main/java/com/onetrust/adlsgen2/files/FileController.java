package com.onetrust.adlsgen2.files;

import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@AllArgsConstructor
@RestController
public class FileController {

    private final FileOperationsService fileOperationsService;

    @PostMapping(path = "/file-upload-gen1")
    public void uploadFile(@RequestParam("file") MultipartFile file,
                           @RequestParam(value = "genVersion",required = false) String genVersion){

        this.fileOperationsService.uploadFile(file,genVersion);
    }
}
