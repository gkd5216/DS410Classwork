import scala.math.{Pi, sqrt, log, max, abs} //Imports scala math operators

case class Neumaier(sum: Double, c: Double)

object HW {

    // note it specifies the input (n) and its type (Int) along with the output
    // type List[Int] ( a list of integers)
    // Question 1
    def q1(n: Int): List[Int] = { //Input is an integer; returns a list
      List.tabulate(n){x => (x+1) * (x+1)} //Uses tabulate to get square numbers
    }

    // In order to get the code to compile, you have to do the same with the rest of the
    // questions, and then fill in code to make them correct.
    // Question 2
    def q2(n: Int): Vector[Double] = { //Input is an integer; returns a vector
      Vector.tabulate(n){x => sqrt(x + 1)} //Uses tabulate and scala math library to get square root 
    }

    // Question 3
    def q3(x: Seq[Double]): Double = { //Input is a sequence of doubles; returns a double 
      x.foldLeft(0.0)(_ + _) //Uses foldleft to return sum of items
    }
    
    // Question 4
    def q4(x: Seq[Double]): Double = { //Input is a sequence of doubles; returns a double
      x.foldLeft(1.0)(_ * _)
    }

    // Question 5
    def q5(x: Seq[Double]): Double = { //Input is a sequence of doubles; returns a double
      x.foldLeft(0.0){(accum, n) => accum + log(n)} //Uses foldleft to get sum of items and scala math library to get logs of the items
    }
    
    // Question 6
    def q6(x: Seq[(Double, Double)]): (Double, Double) = { //Input is a sequence of tuples of doubles; returns a tuple of doubles
      x.foldLeft(0.0, 0.0){case ((sum1, sum2), (x, y)) => (sum1 + x, sum2 + y)} //Uses foldleft to add first element to the sum1 and second element to sum2 to accumulate sums
    }

    // Question 7
    def q7(x: Seq[(Double, Double)]): (Double, Double) = { //Input is a sequence of tuples of doubles; returns a tuple of doubles
      x.foldLeft(0.0, Double.NegativeInfinity){case ((sum1, max2), (x, y)) => //Uses foldleft to get sum of first items and maximum of second items
        (sum1 + x, max(max2, y))}
    }

    // Question 8
    def q8(n: Int): (Int, Int) = { //Input is an integer; returns a tuple
      val a = List.tabulate(n){x => x + 1} //Generates list from 1 to n
      a.foldLeft(0, 1){case ((sum, product), current) => (sum + current, product * current)} 
      // Uses foldleft to get summation and product
    }

    // Question 9
    def q9(x: Seq[Int]): Int = { //Input is sequence of integers; returns an integer
      x.filter {c => c % 2 == 0} //Gets even numbers
        .map {c => c * c} //Squares even numbers
        .foldLeft(0)(_ + _) //Sums squares
    }

    // Question 10
    def q10(x: Seq[Double]): Double = { //Input is a sequence of doubles; returns a double
      x.foldLeft(0.0, 1){case ((accum, index), n) => 
        (accum + n * index, index + 1) //Uses foldleft to use equation input(i) * (i + 1) from i=0 to n-1
      }._1
    }

    // Question 11
    def q11(x: Seq[Double]): Double = {
      val neumaierState = x.foldLeft(Neumaier(0.0, 0.0)) {
        case (Neumaier(sum, c), n) => //Neumaier summation
          val t = sum + n //sum for each n
          val c1 = c + {
            if (abs(sum) >= abs(n)) (sum - t) + n //Update if sum is larger
            else (n - t) + sum //Update if n is larger
          }
          Neumaier(t, c1) //Gets updated state
      }
      neumaierState.sum + neumaierState.c //Gets the corrected summation
    }
}
