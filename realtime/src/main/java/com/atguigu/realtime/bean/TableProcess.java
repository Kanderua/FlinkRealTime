package com.atguigu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/12/25 15:57
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcess {
    private String source_table;
    private String operate_type;
    private String sink_type;
    private String sink_table;
    private String sink_columns;
    private String sink_pk;
    private String sink_extend;
}