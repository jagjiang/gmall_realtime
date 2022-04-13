package com.mintlolly;

import java.lang.reflect.Method;
import java.util.ArrayList;

/**
 * Created on 2022/4/13
 *
 * @author jiangbo
 * Description:
 */
public class Test {
    @org.junit.Test
    public void test(){
        ArrayList<String> as = new ArrayList<>();
        ArrayList<Integer> ai = new ArrayList<>();
        Class asC = as.getClass();
        for (int i = 0; i < asC.getMethods().length; i++) {
        }
        Class aiC = ai.getClass();
        System.out.println(get("sss"));
    }
    public <E> String get(E e){
        return e.toString();
    }
}
