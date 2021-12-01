package com.cjbdi.ws.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

/**
 * @Date 2021/11/19 13:18
 * @Created by ls
 * @Version 1.0.0
 * @Description TODO
 */

@ToString
@Data
@AllArgsConstructor
public class WsBean {
    private String fbId;               //法标ID
    private String host;               //文书记录主机
    private String database;           //文书记录库(省份)
    private String schema;             //文书所属架构(案件类型)
    private String t_c_baah;           //案件案号
    private String t_n_jbfy;           //经办法院ID
    private String t_c_jbfymc;         //经办法院名称
    private String ws_c_nr;            //文书下载URL
    private String ws_n_lb;            //文书类别
    private String t_d_sarq;           //案件收案日期

    private String t_c_stm;
    private String ws_c_mc;
}
