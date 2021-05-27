package queries;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;


public class Tuple2Comparator<F1, F2> implements Comparator<Tuple2<F1, F2>>, Serializable {

        private  Comparator<F1> comp1;
        private  Comparator<F2> comp2;


        public Tuple2Comparator(Comparator<F1> myComp1, Comparator<F2> myComp2 ){
            this.comp1 = myComp1;
            this.comp2 = myComp2;
        }



        @Override
        public int compare(Tuple2<F1, F2> o1, Tuple2<F1, F2> o2) {
            int n;
            n= this.comp1.compare(o1._1(), o2._1());
            if (n!=0){
                return n;
            } else{
                return this.comp2.compare(o1._2(), o2._2());
            }

        }



}
