package com.petuum.ps.common.util;

/**
 * Created by suyuxin on 14-9-3.
 */

public interface MatrixLoader {
    public class Element {
        public int row;
        public int col;
        public float value;
        public boolean isLastEl;
    }

    public Element getNextEl(int workerId);

    public int getN();

    public int getM();

    public int getNNZ();

}
