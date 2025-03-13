# duckboard


# Build exe
## Requirements
requires node on your machine

## install
``` npm install ```
install create node_packages folder and install packages into it.

create build files and artifacts
``` npm run dump ```
This dump command creates ./build directory that contains the copied Streamlit app files, dumped installed packages, Pyodide runtime, Electron app files, etc.

``` npm run serve ```
This command is just a wrapper of electron command as you can see at the "scripts" field in the package.json. It launches Electron and starts the app with ./build/electron/main.js, which is specified at the "main" field in the package.json.

``` npm run app:dist ```
This command bundles the ./build directory created in the step above into application files (.app, .exe, .dmg etc.) in the ./dist directory. To customize the built app, e.g. setting the icon, follow the electron-builder instructions.