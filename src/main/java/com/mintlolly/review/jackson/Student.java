package com.mintlolly.review.jackson;

/**
 * Created on 2022/3/10
 *
 * @author jiangbo
 * Description:
 */
public class Student {

    private String name;
    private int age;

    //no Creators, like default construct, exist 不写的话会报错
    public Student(){

    }
    public Student(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Student{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
