object Tester extends App {
   
    val q1answer: List[Int] = HW.q1(3)

    val q2answer: Vector[Double] = HW.q2(4)

    val q3answer_a: Double = HW.q3(List(1.0,2.0,3.0))
    val q3answer_b: Double = HW.q3(Vector(3.0,4.0,5.0))

    val q4answer_a: Double = HW.q4(List(1.0,2.0,3.0))
    val q4answer_b: Double = HW.q4(Vector(3.0,4.0,5.0))

    val q5answer_a: Double = HW.q5(List(1.0,2.0,3.0))
    val q5answer_b: Double = HW.q5(Vector(3.0,4.0,5.0))

    val q6answer_a: (Double, Double) = HW.q6(List((1.0, 2.0), (2.0, 3.0), (3.0, 4.0)))
    val q6answer_b: (Double, Double) = HW.q6(Vector((3.0, 4.0), (5.0, 6.0)))

    val q7answer_a: (Double, Double) = HW.q7(List((1.0, 2.0), (2.0, 3.0), (3.0, 4.0)))
    val q7answer_b: (Double, Double) = HW.q7(Vector((3.0, 4.0), (5.0, 6.0)))

    val q8answer: (Int, Int) = HW.q8(4)

    val q9answer_a: Int = HW.q9(List(4,5,8,7))
    val q9answer_b: Int = HW.q9(Vector(4,5,8,7))

    val q10answer_a: Double = HW.q10(List(4.0,5.0,8.0,7.0))
    val q10answer_b: Double = HW.q10(Vector(4.0,5.0,8.0,7.0))

    val q11answer_a: Double = HW.q11(List(1.0, 1e100, 1.0, -1e100))
    val q11answer_b: Double = HW.q11(Vector(1.0, 1e100, 1.0, -1e100))
}
