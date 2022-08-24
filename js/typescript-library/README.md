# typescript-library

## Setup

### Use the library in your project

For the time being, the project is not published anywhere and need to be compiled and publish locally.

Compile: `npm run build` in the typescript-library directory. 

Publish locally: `npm link`. It creates a symbolic link to the entire folder to the global namespace

Check global packages (namespace): `npm list -g`, the project should appear in the list.

### Client project
Add to the project with `npm link aitm-js-query@1.0.0`

To compile our code, we'll need to run the tsc command using npx, the Node package executer. tsc will read the tsconfig.json 
in the current directory, and apply the configuration against the TypeScript compiler to generate the compiled JavaScript code. 

Execute: `npx tsc`

Check out build/index.js for compiled code.

## Links

https://www.tsmean.com/articles/how-to-write-a-typescript-library/

## Testing
Full command to execute in js/ dir. 
```
cd typescript-library && npm run build && npm link && cd ../client-typescript-library && npm link aitm-js-query@1.0.0 && cd ..
```

In IntelliJ, run js/client-typescript-library/index.ts or execute `npm run test` in client-typescript-library.
