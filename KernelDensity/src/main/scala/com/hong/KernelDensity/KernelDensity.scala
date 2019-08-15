package com.hong.KernelDensity

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

/**
  * 基于源码，实现对多维度数据的支持
  *
  * Kernel density estimation. Given a sample from a population, estimate its probability density
  * function at each of the given evaluation points using kernels. Only Gaussian kernel is supported.
  *
  * Scala example:
  *
  * {{{
  * val sample = sc.parallelize(Seq(0.0, 1.0, 4.0, 4.0))
  * val kd = new com.finest.KernelDensity.KernelDensity()
  *   .setSample(sample)
  *   .setBandwidth(3.0)
  * val densities = kd.estimate(Array(-1.0, 2.0, 5.0))
  * }}}
  */

class KernelDensity extends Serializable {

  import KernelDensity._

  /** Bandwidth of the kernel function. */
  private var bandwidth: Double = 1.0

  /** A sample from a population. */
  private var sample: RDD[Array[Double]] = _

  /**
    * Sets the bandwidth (standard deviation) of the Gaussian kernel (default: `1.0`).
    */
  def setBandwidth(bandwidth: Double): this.type = {
    require(bandwidth > 0, s"Bandwidth must be positive, but got $bandwidth.")
    this.bandwidth = bandwidth
    this
  }

  /**
    * Sets the sample to use for density estimation.
    */
  def setSample(sample: RDD[Array[Double]]): this.type = {
    this.sample = sample
    this
  }

  /**
    * Sets the sample to use for density estimation (for Java users).
    */
  def setSample(sample: JavaRDD[Array[Double]]): this.type = {
    this.sample = sample.rdd.asInstanceOf[RDD[Array[Double]]]
    this
  }

  /**
    * Estimates probability density function at the given array of points.
    */
  def estimate(points: Array[Array[Double]]): Array[Double] = {
    val sample = this.sample
    val bandwidth = this.bandwidth

    require(sample != null, "Must set sample before calling estimate.")

    val n = points.length
    // This gets used in each Gaussian PDF computation, so compute it up front
    val logStandardDeviationPlusHalfLog2Pi = math.log(bandwidth) + 0.5 * math.log(2 * math.Pi)
    val (densities, count) = sample.aggregate((new Array[Double](n), 0L))(
      // y是对sample的遍历
      // x是存放每次返回的Tuple，初始值即为传入的(new Array[Double](n), 0L)
      // 每次都将上一轮返回的x作为这一轮的x输入
      (x, y) => {
        var i = 0
        while (i < n) {
          var multiply:Double = 1
          for (m <- 0 until y.length) {multiply *= normPdf(y(m), bandwidth, logStandardDeviationPlusHalfLog2Pi, points(i)(m))}
          x._1(i) += multiply
          i += 1
        }
        (x._1, x._2 + 1)
      },
      (x, y) => { // 这里是对所有分区的结果进行聚合
        blas.daxpy(n, 1.0, y._1, 1, x._1, 1)
        (x._1, x._2 + y._2)
      })
    blas.dscal(n, 1.0 / count, densities, 1)
    densities
  }
}

private object KernelDensity {

  /** Evaluates the PDF of a normal distribution. */
  def normPdf(
               mean: Double,
               standardDeviation: Double,
               logStandardDeviationPlusHalfLog2Pi: Double,
               x: Double): Double = {
    val x0 = x - mean
    val x1 = x0 / standardDeviation
    val logDensity = -0.5 * x1 * x1 - logStandardDeviationPlusHalfLog2Pi
    math.exp(logDensity)
  }
}
