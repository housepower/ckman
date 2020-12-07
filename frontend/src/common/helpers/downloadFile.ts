import { Project } from '../constants';

export function downloadFile(url: string, params: {[key: string]: any} = {}, method = 'get', target = '') {
  if (!url) return;
  const form = document.createElement('form');
  form.style.display = 'none';
  form.method = method;
  form.action = Project.serviceName ? `/${Project.serviceName}${url}` : url; // FIXME 后续需要统一处理
  form.target = target;
  Object.entries(params).filter(([, value]) => value !== undefined).forEach(param => {
    const ipt = document.createElement('input');
    ipt.name = param[0];
    ipt.value = param[1];
    form.appendChild(ipt);
  });
  document.body.appendChild(form);
  form.submit();
  document.body.removeChild(form);
}
