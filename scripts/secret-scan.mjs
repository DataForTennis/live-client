import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';

const files = execFileSync('git', ['ls-files', '--cached', '--others', '--exclude-standard', '-z'], {
  encoding: 'utf8',
})
  .split('\0')
  .filter(Boolean)
  .filter((file) => !file.startsWith('output/') && file !== 'package-lock.json');

const patterns = [
  ['GitHub token', /gh[pousr]_[A-Za-z0-9]{30,}/g],
  ['AWS access key', /AKIA[0-9A-Z]{16}/g],
  ['private key', /-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/g],
  ['JWT', /eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}/g],
  ['bearer credential', /Bearer\s+(?!\[REDACTED\])[A-Za-z0-9._~+/=-]{16,}/gi],
];

const findings = [];
for (const file of files) {
  let text;
  try {
    text = readFileSync(file, 'utf8');
  } catch {
    continue;
  }
  for (const [name, pattern] of patterns) {
    pattern.lastIndex = 0;
    if (pattern.test(text)) findings.push(`${file}: ${name}`);
  }
}

if (findings.length > 0) {
  console.error(`Secret scan found ${findings.length} potential issue(s):`);
  for (const finding of findings) console.error(`- ${finding}`);
  process.exitCode = 1;
} else {
  console.log(`Secret scan passed: ${files.length} files checked, 0 findings.`);
}
