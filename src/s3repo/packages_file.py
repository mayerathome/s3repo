from __future__ import absolute_import
import gzip
import io
from s3repo.field_set import FieldSet

from boto.s3.key import Key

def GzipCompress(contents):
  out_stream = io.BytesIO()
  gzip.GzipFile(fileobj=out_stream, mode="wb", compresslevel=9).write(contents)
  return out_stream.getvalue()

class PackagesFile(object):
  def __init__(self, contents):
    self.packages = []
    for package_str in contents.decode("utf-8").split("\n\n"):
      if package_str == "":
        continue
      self.packages.append(FieldSet(package_str))

  @classmethod
  def Load(cls, bucket, bucket_path):
    return cls(Key(bucket=bucket, name=bucket_path).get_contents_as_string())

  def Store(self, bucket, bucket_path, acl):
    key = Key(bucket=bucket, name=bucket_path)
    contents = str(self).encode("utf-8")
    key.set_contents_from_string(contents, policy=acl)

    key = Key(bucket=bucket, name=bucket_path + ".gz")
    contents_gz = GzipCompress(contents)
    key.set_contents_from_string(contents_gz, policy=acl)

    return contents, contents_gz

  def AddPackage(self, metadata):
    self.packages.append(metadata)

  def RemovePackage(self, name):
    for package in self.packages[:]:
      if package["Package"] == name:
        self.packages.remove(package)
        yield package["Filename"]

  def __str__(self):
    return "\n\n".join(map(str, self.packages))
