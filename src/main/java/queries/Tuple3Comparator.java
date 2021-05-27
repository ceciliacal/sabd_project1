package queries;


import scala.Tuple3;

import java.io.Serializable;
import java.util.Comparator;




public class Tuple3Comparator<F1, F2, F3> implements Comparator<Tuple3<F1, F2, F3>>, Serializable {

    //private Comparator<V> comparator;
    private  Comparator<F1> comp1;
    private  Comparator<F2> comp2;
    private  Comparator<F3> comp3;



    public Tuple3Comparator(Comparator<F1> myComp1, Comparator<F2> myComp2, Comparator<F3> myComp3 ){
        this.comp1 = myComp1;
        this.comp2 = myComp2;
        this.comp3 = myComp3;
    }



    @Override
    public int compare(Tuple3<F1, F2, F3> o1, Tuple3<F1, F2, F3> o2) {
        int n;
        n= this.comp1.compare(o1._1(), o2._1());
        if (n!=0) return n;
        n= this.comp2.compare(o1._2(), o2._2());
        if (n!=0) return n;
        return this.comp3.compare(o1._3(), o2._3());
        //return this.comp1.compare(o1._1(), o2._1());

    }
}

