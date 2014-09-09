package com.petuum.ps.common.test;

/**
 * Created by suyuxin on 14-8-27.
 */
public class ThreadTest {

    private static int a = 10;

    private static class MyThread implements Runnable {

        public void run() {
            System.out.println(a);
            try {
                Thread.sleep(4000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            a = 20;
        }
    }

    private static Thread one = new Thread(new MyThread());

    private static Thread thread = new Thread(new Runnable() {
        public void run() {
            System.out.println(a);
            try {
                Thread.sleep(4000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            a = 30;
        }
    });

    public static void main(String[] args) throws InterruptedException {
        //thread.start();
        one.start();
        Thread.sleep(6000);
        System.out.println(a);
    }
}
