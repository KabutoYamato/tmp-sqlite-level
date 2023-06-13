export class ModuleError extends Error {

    code?:string;
    expected?:boolean;
    transient?:boolean;
    cause?:Error;

    /**
     * @param  message Error message
     * @param  options Options
     */
    constructor (message: string, options: { code?: string;  }) {
      super(message || '')
  
      if (typeof options === 'object' && options !== null) {
        if (options.code) this.code = String(options.code)
      }
  
      if (Error.captureStackTrace) {
        Error.captureStackTrace(this, this.constructor)
      }
    }
}