package com.cjbdi.ws.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

/**
 * @Date 2021/11/24 14:57
 * @Created by ls
 * @Version 1.0.0
 * @Description TODO
 */

@Data
@AllArgsConstructor
@ToString
public class WsBeanDownloaded {
    private WsBean wsBean;              //文书元数据
    private String base64File;          //文书原文件,为Base64编码的字节数组
}
