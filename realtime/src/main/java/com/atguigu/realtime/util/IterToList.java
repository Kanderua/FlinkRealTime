package com.atguigu.realtime.util;

import java.util.ArrayList;
import java.util.List;

public class IterToList {
    public static <T> List<T> toList(Iterable<T> it) {
        ArrayList<T> result = new ArrayList<>();
        it.forEach(result::add);
        return result;
    }
}
