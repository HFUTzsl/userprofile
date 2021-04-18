package com.qf.bigdata.profile.nlp.hanlp

import java.io.{FileInputStream, InputStream, OutputStream}
import java.net.URI

import com.hankcs.hanlp.corpus.io.{IIOAdapter, IOUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * @Author shanlin
  * @Date Created in  2021/1/18 11:08
  *
  */
class HadoopFileAdapter extends IIOAdapter {

  override def open(path: String): InputStream = {
    val conf = new Configuration()
    val fs: FileSystem = FileSystem.get(URI.create(path), conf)
    if (fs.exists(new Path(path))) {
      fs.open(new Path(path))
    } else if (IOUtil.isResource(path)) {
      IOUtil.getResourceAsStream("/" + path)
    } else {
      new FileInputStream(path)
    }
  }

  override def create(path: String): OutputStream = {
    val conf = new Configuration()
    val fs: FileSystem = FileSystem.get(URI.create(path), conf)
    fs.create(new Path(path))
  }
}
