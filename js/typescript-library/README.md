## NPM

### Library

In the root directory: `tsc` to compile the project. 

Local publish (do it only once): `npm link`
-> create a symbolic link to the entire folder to the global namespace

Check global packages (namespace): `npm list -g`, the project should appear in the list.

### Client
Add to the project ? npm link myproject

To compile our code, we'll need to run the tsc command using npx, the Node package executer. tsc will read the tsconfig.json in the current directory, and apply the configuration against the TypeScript compiler to generate the compiled JavaScript code. 
--> npx tsc

Check out build/index.js for compiled code

## Links

https://www.tsmean.com/articles/how-to-write-a-typescript-library/
