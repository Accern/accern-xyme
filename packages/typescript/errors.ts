import { Response } from 'node-fetch';
export class AccessDenied extends Error {
    constructor(message: string) {
        super(message);

        Object.setPrototypeOf(this, AccessDenied.prototype);
    }
}

export class ServerSideError extends Error {
    constructor(message: string) {
        super(message);

        Object.setPrototypeOf(this, ServerSideError.prototype);
    }
}

export class KeyError extends Error {
    constructor(message: string) {
        super(message);

        Object.setPrototypeOf(this, KeyError.prototype);
    }
}

export class HTTPResponseError extends Error {
    response: Response;
    constructor(response: Response) {
        super(
            `HTTP Error Response: ${response.status} ${response.statusText}`
        );
        this.response = response;
        Object.setPrototypeOf(this, HTTPResponseError.prototype);
    }
}
