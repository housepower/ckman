export function readFile(file: File) {
  return new Promise<string>(((resolve, reject) => {
    if (file) {
      const reader = new FileReader();
      reader.onload = () => {
        resolve(reader.result as string);
        reader.onload = null;
      };
      reader.readAsText(file);
    } else {
      reject(new Error('读取文件失败'));
    }
  }));
}
